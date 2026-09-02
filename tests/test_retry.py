"""Retry policies: what gets retried, how long we wait, and when we stop."""

from __future__ import annotations

import random

import pytest

from flowforge.clock import ManualClock
from flowforge.errors import (
    CircuitOpen,
    PermanentError,
    RetryExhausted,
    TransientError,
)
from flowforge.retry import (
    NO_RETRY,
    CircuitBreaker,
    CircuitState,
    ExponentialBackoff,
    FixedDelay,
    JitteredExponentialBackoff,
    policy_from_spec,
    retry_call,
    retryable_status,
)


def test_exponential_delays_are_exact_and_capped():
    policy = ExponentialBackoff(max_attempts=6, base=0.5, factor=2.0, max_delay=4.0)
    assert [policy.delay_for(n) for n in range(1, 6)] == [0.5, 1.0, 2.0, 4.0, 4.0]


def test_fixed_delay_is_constant():
    policy = FixedDelay(max_attempts=3, delay=2.5)
    assert [policy.delay_for(n) for n in (1, 2, 3)] == [2.5, 2.5, 2.5]


def test_jitter_is_deterministic_under_an_injected_rng():
    first = JitteredExponentialBackoff(base=1.0, rng=random.Random(7))
    second = JitteredExponentialBackoff(base=1.0, rng=random.Random(7))
    a = [first.delay_for(n) for n in (1, 2, 3)]
    b = [second.delay_for(n) for n in (1, 2, 3)]
    assert a == b
    # Jitter of 0.5 keeps each delay inside [d/2, d].
    for attempt, delay in zip((1, 2, 3), a):
        ceiling = 1.0 * 2 ** (attempt - 1)
        assert ceiling / 2 <= delay <= ceiling
    full = JitteredExponentialBackoff(base=1.0, full=True, rng=random.Random(3))
    assert 0.0 <= full.delay_for(1) <= 1.0


def test_value_error_from_bad_input_is_not_retried():
    """The headline rule: a bad value fails once, not five times."""
    policy = ExponentialBackoff(max_attempts=5)
    calls = []

    def bad_input(attempt: int) -> None:
        calls.append(attempt)
        raise ValueError("row 412: 'N/A' is not a number")

    with pytest.raises(ValueError):
        retry_call(bad_input, policy, clock=ManualClock())
    assert calls == [1]
    decision = policy.should_retry(1, ValueError("nope"))
    assert decision.retry is False
    assert decision.reason == "non-retryable:ValueError"


def test_permanent_error_is_never_retried_even_if_allowlisted():
    policy = ExponentialBackoff(max_attempts=4, retry_on=(Exception,))
    assert policy.is_retryable(PermanentError("404")) is False
    assert policy.is_retryable(TransientError("503")) is True


def test_retries_stop_at_max_attempts_and_sleep_the_right_amounts():
    clock = ManualClock()
    policy = ExponentialBackoff(max_attempts=3, base=0.5, factor=2.0)
    attempts = []

    def flaky(attempt: int) -> None:
        attempts.append(attempt)
        raise TransientError("still down")

    with pytest.raises(RetryExhausted) as excinfo:
        retry_call(flaky, policy, clock=clock)
    assert attempts == [1, 2, 3]
    assert clock.sleeps == [0.5, 1.0]
    assert excinfo.value.attempts == 3
    assert isinstance(excinfo.value.last_error, TransientError)


def test_retry_stops_when_the_next_delay_would_pass_the_deadline():
    clock = ManualClock()
    policy = ExponentialBackoff(max_attempts=10, base=10.0)
    decision = policy.should_retry(
        1, TransientError("down"), now=clock.monotonic(), deadline=5.0
    )
    assert decision.retry is False
    assert decision.reason.startswith("deadline-would-be-exceeded")

    calls = []

    def slow(attempt: int) -> None:
        calls.append(attempt)
        raise TransientError("down")

    with pytest.raises(TransientError):
        retry_call(slow, policy, clock=clock, deadline=5.0)
    assert calls == [1], "no retry should have been scheduled past the deadline"
    assert clock.sleeps == []


def test_retry_stops_once_the_deadline_has_already_passed():
    clock = ManualClock(start=100.0)
    policy = ExponentialBackoff(max_attempts=5, base=0.1)
    decision = policy.should_retry(
        1, TransientError("x"), now=clock.monotonic(), deadline=50.0
    )
    assert decision.reason == "deadline-passed"


def test_successful_call_returns_the_value_and_one_attempt_record():
    value, records = retry_call(lambda attempt: attempt * 2, NO_RETRY, clock=ManualClock())
    assert value == 2
    assert [r.reason for r in records] == ["ok"]


def test_recovery_after_two_failures():
    clock = ManualClock()

    def eventually(attempt: int) -> str:
        if attempt < 3:
            raise ConnectionError("reset by peer")
        return "ok"

    value, records = retry_call(
        eventually, ExponentialBackoff(max_attempts=4, base=0.01), clock=clock
    )
    assert value == "ok"
    assert len(records) == 3
    assert records[-1].reason == "ok"


def test_circuit_breaker_opens_then_half_opens_then_closes():
    clock = ManualClock()
    breaker = CircuitBreaker("api", failure_threshold=2, reset_timeout=30.0, clock=clock)
    assert breaker.state is CircuitState.CLOSED
    breaker.record_failure()
    assert breaker.state is CircuitState.CLOSED
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitOpen):
        breaker.check()
    clock.advance(31.0)
    assert breaker.state is CircuitState.HALF_OPEN
    breaker.check()  # one probe is allowed through
    breaker.record_success()
    assert breaker.state is CircuitState.CLOSED
    assert breaker.failures == 0


def test_open_circuit_short_circuits_the_call():
    clock = ManualClock()
    breaker = CircuitBreaker("api", failure_threshold=1, reset_timeout=60.0, clock=clock)
    breaker.record_failure()
    calls = []
    with pytest.raises(CircuitOpen):
        retry_call(
            lambda attempt: calls.append(attempt),
            NO_RETRY,
            clock=clock,
            breaker=breaker,
        )
    assert calls == [], "the call must not be made while the circuit is open"


def test_retryable_status_codes():
    assert retryable_status(503) is True
    assert retryable_status(429) is True
    assert retryable_status(500) is True
    assert retryable_status(501) is False
    assert retryable_status(404) is False
    assert retryable_status(200) is False


def test_policy_from_spec_builds_what_the_dsl_asks_for():
    assert policy_from_spec(None) is NO_RETRY
    assert policy_from_spec(1) is NO_RETRY
    assert policy_from_spec(3).max_attempts == 3
    fixed = policy_from_spec({"attempts": 2, "strategy": "fixed", "delay": 4})
    assert isinstance(fixed, FixedDelay) and fixed.delay_for(1) == 4
    jitter = policy_from_spec({"attempts": 3, "strategy": "jitter", "seed": 1})
    assert isinstance(jitter, JitteredExponentialBackoff)
    with pytest.raises(ValueError):
        policy_from_spec({"attempts": 2, "strategy": "wishful"})
