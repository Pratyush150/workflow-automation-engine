"""Retry policies, backoff, and a circuit breaker.

The rule this module exists to enforce: **retry only what can succeed later.**

A ``ValueError`` raised because row 412 has "N/A" in a numeric column will
raise the identical ``ValueError`` on attempt five. Retrying it costs five
times the runtime, five identical stack traces in the log, five times the load
on whatever you were reading from, and it delays the moment a human sees the
real message. Worse, blanket retries turn a fast, obvious failure into a slow,
ambiguous one -- the run now dies of a deadline instead of the bad row, and the
alert says "timeout" instead of "bad value in column ``amount``".

So policies retry on an explicit exception allowlist. The default allowlist is
:class:`~flowforge.errors.TransientError` plus the usual OS-level transport
errors. :class:`~flowforge.errors.PermanentError` is never retried, even if a
caller adds it to the allowlist by mistake.

Two more properties everything here has:

* **Deadline aware.** A retry is only scheduled if the sleep plus a minimum
  execution budget still fits inside the run deadline. Sleeping 60s when 12s
  of budget remain just converts a retryable failure into a deadline failure.
* **Deterministic under an injected RNG.** Jitter takes a ``random.Random``.
  Tests seed it and assert the exact delay sequence.
"""

from __future__ import annotations

import random
import socket
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, List, Optional, Tuple, TypeVar

from .clock import SYSTEM_CLOCK, Clock
from .errors import CircuitOpen, PermanentError, RetryExhausted, TransientError

__all__ = [
    "CircuitBreaker",
    "CircuitState",
    "ExponentialBackoff",
    "FixedDelay",
    "JitteredExponentialBackoff",
    "NO_RETRY",
    "RetryDecision",
    "RetryPolicy",
    "retry_call",
]

T = TypeVar("T")

#: Transport-level failures that are worth another attempt regardless of where
#: they came from. Kept narrow on purpose.
# ``socket.timeout`` is an alias of ``TimeoutError`` on modern Python; dict
# ordering de-duplicates without assuming which version we are on.
DEFAULT_RETRYABLE: Tuple[type, ...] = tuple(
    dict.fromkeys(
        (TransientError, ConnectionError, TimeoutError, socket.timeout, OSError)
    )
)


@dataclass(frozen=True)
class RetryDecision:
    """The outcome of asking a policy "should I try again?".

    ``reason`` is a short machine-ish string. It ends up in the structured log
    and in the run state, so "why did this stop retrying?" is answerable after
    the fact instead of being guessed from timings.
    """

    retry: bool
    delay: float = 0.0
    reason: str = ""


@dataclass(frozen=True)
class RetryPolicy:
    """Base policy: attempt budget plus an exception allowlist.

    Subclasses implement :meth:`delay_for`. The base class is a valid policy in
    its own right -- it retries immediately, with no backoff.
    """

    max_attempts: int = 1
    retry_on: Tuple[type, ...] = DEFAULT_RETRYABLE
    #: Always wins over ``retry_on``. ``PermanentError`` is added automatically.
    never_retry: Tuple[type, ...] = ()
    #: Assume a retried call needs at least this long to be worth starting.
    min_execution_budget: float = 0.0

    def delay_for(self, attempt: int) -> float:
        """Seconds to wait before ``attempt`` + 1. ``attempt`` is 1-based."""
        return 0.0

    def is_retryable(self, exc: BaseException) -> bool:
        """Allowlist check. ``PermanentError`` is never retryable."""
        if isinstance(exc, PermanentError):
            return False
        if self.never_retry and isinstance(exc, tuple(self.never_retry)):
            return False
        return isinstance(exc, tuple(self.retry_on))

    def should_retry(
        self,
        attempt: int,
        exc: BaseException,
        *,
        now: float = 0.0,
        deadline: Optional[float] = None,
    ) -> RetryDecision:
        """Full decision: allowlist, attempt budget, then the deadline."""
        if not self.is_retryable(exc):
            return RetryDecision(False, 0.0, f"non-retryable:{type(exc).__name__}")
        if attempt >= self.max_attempts:
            return RetryDecision(False, 0.0, f"attempts-exhausted:{self.max_attempts}")
        delay = self.delay_for(attempt)
        if deadline is not None:
            remaining = deadline - now
            if remaining <= 0:
                return RetryDecision(False, 0.0, "deadline-passed")
            if delay + self.min_execution_budget > remaining:
                return RetryDecision(
                    False,
                    0.0,
                    f"deadline-would-be-exceeded:need={delay + self.min_execution_budget:.3f}"
                    f",left={remaining:.3f}",
                )
        return RetryDecision(True, delay, f"retry:attempt={attempt + 1}")


@dataclass(frozen=True)
class FixedDelay(RetryPolicy):
    """Wait the same amount between every attempt.

    Fine for a resource that is either up or down. Bad for a service that is
    overloaded: every client retrying on the same fixed interval re-synchronises
    the stampede that caused the overload.
    """

    delay: float = 1.0

    def delay_for(self, attempt: int) -> float:
        return self.delay


@dataclass(frozen=True)
class ExponentialBackoff(RetryPolicy):
    """``base * factor ** (attempt - 1)``, capped at ``max_delay``."""

    base: float = 0.5
    factor: float = 2.0
    max_delay: float = 30.0

    def delay_for(self, attempt: int) -> float:
        raw = self.base * (self.factor ** max(0, attempt - 1))
        return min(raw, self.max_delay)


@dataclass(frozen=True)
class JitteredExponentialBackoff(ExponentialBackoff):
    """Exponential backoff with jitter, to break up synchronised retries.

    ``jitter`` is a fraction of the computed delay. With ``full=True`` the delay
    is uniform in ``[0, d]`` (AWS "full jitter"); otherwise it is uniform in
    ``[d * (1 - jitter), d]``.

    The RNG is injected, so a test can assert the exact sequence rather than a
    range. ``JitteredExponentialBackoff(base=1.0, rng=random.Random(7))``
    produces the same three delays on every machine and every run.
    """

    jitter: float = 0.5
    full: bool = False
    rng: random.Random = field(default_factory=lambda: random.Random(0))

    def delay_for(self, attempt: int) -> float:
        base = super().delay_for(attempt)
        if self.full:
            return self.rng.uniform(0.0, base)
        low = base * (1.0 - min(max(self.jitter, 0.0), 1.0))
        return self.rng.uniform(low, base)


#: Run once, never retry. The default for every task.
NO_RETRY = RetryPolicy(max_attempts=1)


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Stop hammering a dependency that is clearly down.

    Retries help with a blip. They actively hurt when a service is in trouble:
    every workflow in the fleet retrying three times turns a partial outage into
    a full one. After ``failure_threshold`` consecutive failures the circuit
    opens and calls fail immediately with :class:`CircuitOpen` -- cheap, fast,
    and obvious in the log. After ``reset_timeout`` one probe is allowed
    through (``half_open``); if it succeeds the circuit closes.
    """

    def __init__(
        self,
        name: str = "default",
        *,
        failure_threshold: int = 5,
        reset_timeout: float = 30.0,
        clock: Clock = SYSTEM_CLOCK,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._clock = clock
        self._failures = 0
        self._opened_at: Optional[float] = None
        self._state = CircuitState.CLOSED

    @property
    def state(self) -> CircuitState:
        """Current state, re-evaluated against the clock."""
        if self._state is CircuitState.OPEN and self._opened_at is not None:
            if self._clock.monotonic() - self._opened_at >= self.reset_timeout:
                self._state = CircuitState.HALF_OPEN
        return self._state

    @property
    def failures(self) -> int:
        return self._failures

    def check(self) -> None:
        """Raise :class:`CircuitOpen` if the call must not be made."""
        if self.state is CircuitState.OPEN:
            assert self._opened_at is not None
            left = self.reset_timeout - (self._clock.monotonic() - self._opened_at)
            raise CircuitOpen(self.name, max(0.0, left))

    def record_success(self) -> None:
        self._failures = 0
        self._opened_at = None
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self._failures += 1
        if self._state is CircuitState.HALF_OPEN or (
            self._failures >= self.failure_threshold
        ):
            self._state = CircuitState.OPEN
            self._opened_at = self._clock.monotonic()

    def reset(self) -> None:
        self._failures = 0
        self._opened_at = None
        self._state = CircuitState.CLOSED


@dataclass
class AttemptRecord:
    """One attempt, for logging and assertions."""

    attempt: int
    error: Optional[BaseException]
    delay: float
    reason: str


def retry_call(
    fn: Callable[[int], T],
    policy: RetryPolicy = NO_RETRY,
    *,
    clock: Clock = SYSTEM_CLOCK,
    deadline: Optional[float] = None,
    breaker: Optional[CircuitBreaker] = None,
    on_attempt: Optional[Callable[[AttemptRecord], None]] = None,
) -> Tuple[T, List[AttemptRecord]]:
    """Call ``fn(attempt)`` under ``policy``.

    ``fn`` receives the 1-based attempt number, so a task can log or vary
    behaviour per attempt. Returns ``(value, attempts)``. On final failure
    raises the underlying exception, wrapped in :class:`RetryExhausted` only
    when more than one attempt was made -- an unwrapped first-attempt failure
    keeps the caller's own exception type intact.
    """
    records: List[AttemptRecord] = []
    attempt = 0
    while True:
        attempt += 1
        try:
            if breaker is not None:
                breaker.check()
            value = fn(attempt)
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            if breaker is not None and not isinstance(exc, CircuitOpen):
                breaker.record_failure()
            decision = policy.should_retry(
                attempt, exc, now=clock.monotonic(), deadline=deadline
            )
            record = AttemptRecord(attempt, exc, decision.delay, decision.reason)
            records.append(record)
            if on_attempt:
                on_attempt(record)
            if not decision.retry:
                if attempt > 1:
                    raise RetryExhausted(attempt, exc) from exc
                raise
            clock.sleep(decision.delay)
            continue
        if breaker is not None:
            breaker.record_success()
        record = AttemptRecord(attempt, None, 0.0, "ok")
        records.append(record)
        if on_attempt:
            on_attempt(record)
        return value, records


def retryable_status(status: int) -> bool:
    """HTTP status codes worth retrying.

    429 and 5xx (except 501 Not Implemented, which is permanent). A 4xx is your
    request being wrong; sending it again unchanged is superstition.
    """
    if status == 429:
        return True
    return 500 <= status < 600 and status != 501


def policy_from_spec(spec: object) -> RetryPolicy:
    """Build a policy from a plain value, used by the YAML/JSON DSL.

    ``3`` -> three attempts with exponential backoff.
    ``{"attempts": 4, "strategy": "fixed", "delay": 2}`` -> explicit.
    """
    if spec is None:
        return NO_RETRY
    if isinstance(spec, RetryPolicy):
        return spec
    if isinstance(spec, int):
        if spec <= 1:
            return NO_RETRY
        return ExponentialBackoff(max_attempts=spec)
    if isinstance(spec, dict):
        attempts = int(spec.get("attempts", spec.get("max_attempts", 1)))
        strategy = str(spec.get("strategy", "exponential")).lower()
        if attempts <= 1:
            return NO_RETRY
        if strategy == "fixed":
            return FixedDelay(max_attempts=attempts, delay=float(spec.get("delay", 1.0)))
        if strategy in ("jitter", "jittered", "exponential_jitter"):
            return JitteredExponentialBackoff(
                max_attempts=attempts,
                base=float(spec.get("base", 0.5)),
                factor=float(spec.get("factor", 2.0)),
                max_delay=float(spec.get("max_delay", 30.0)),
                jitter=float(spec.get("jitter", 0.5)),
                rng=random.Random(int(spec.get("seed", 0))),
            )
        if strategy == "exponential":
            return ExponentialBackoff(
                max_attempts=attempts,
                base=float(spec.get("base", 0.5)),
                factor=float(spec.get("factor", 2.0)),
                max_delay=float(spec.get("max_delay", 30.0)),
            )
        raise ValueError(f"unknown retry strategy {strategy!r}")
    raise TypeError(f"cannot build a retry policy from {spec!r}")
