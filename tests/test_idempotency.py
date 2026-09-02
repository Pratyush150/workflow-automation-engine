"""Idempotency: keys, stores, and not sending the email twice."""

from __future__ import annotations

import json
import os

import pytest

from flowforge import (
    Executor,
    ExponentialBackoff,
    TaskContext,
    TaskStatus,
    Workflow,
    content_key,
)
from flowforge.clock import ManualClock
from flowforge.errors import TransientError
from flowforge.idempotency import (
    AmbiguousReplay,
    IdempotencyGuard,
    JsonFileStore,
    MemoryStore,
    canonical_json,
    digest,
)


def test_content_keys_are_stable_and_content_addressed():
    a = content_key("send", customer="c-1", amount=120)
    b = content_key("send", amount=120, customer="c-1")
    c = content_key("send", customer="c-2", amount=120)
    assert a == b, "key order must not matter"
    assert a != c
    assert a.startswith("send:")


def test_canonical_json_is_order_independent_and_handles_odd_types():
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'
    assert digest([1, 2]) == digest([1, 2])
    assert digest({"x": {1, 2}}) == digest({"x": {2, 1}})
    assert len(digest("anything")) == 16


def test_guard_runs_the_function_once_per_key():
    guard = IdempotencyGuard(MemoryStore())
    calls = []

    def send() -> str:
        calls.append(1)
        return "sent"

    first, cached_first = guard.call("k1", send)
    second, cached_second = guard.call("k1", send)
    assert (first, cached_first) == ("sent", False)
    assert (second, cached_second) == ("sent", True)
    assert len(calls) == 1


def test_a_failed_key_is_safe_to_run_again():
    guard = IdempotencyGuard(MemoryStore())
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) == 1:
            raise TransientError("smtp refused")
        return "sent"

    with pytest.raises(TransientError):
        guard.call("k", flaky)
    assert guard.lookup("k").status == "failed"
    value, cached = guard.call("k", flaky)
    assert (value, cached) == ("sent", False)
    assert len(attempts) == 2


def test_a_key_left_in_flight_is_ambiguous_and_the_policy_decides():
    store = MemoryStore()
    # Simulate a process killed between "begin" and "complete".
    IdempotencyGuard(store).begin("k", run_id="dead-run", task_id="email")

    rerun = IdempotencyGuard(store, on_ambiguous="rerun")
    assert rerun.begin("k", run_id="new").state == "fresh"

    store2 = MemoryStore()
    IdempotencyGuard(store2).begin("k", run_id="dead-run")
    skip = IdempotencyGuard(store2, on_ambiguous="skip")
    assert skip.begin("k", run_id="new").state == "cached"

    store3 = MemoryStore()
    IdempotencyGuard(store3).begin("k", run_id="dead-run")
    strict = IdempotencyGuard(store3, on_ambiguous="error")
    with pytest.raises(AmbiguousReplay) as excinfo:
        strict.begin("k", run_id="new")
    assert "in-flight" in str(excinfo.value)


def test_json_file_store_round_trips_and_writes_atomically(tmp_path):
    path = str(tmp_path / "idem.json")
    guard = IdempotencyGuard(JsonFileStore(path))
    guard.call("email:1", lambda: {"sent": 3})

    assert os.path.exists(path)
    assert not [p for p in os.listdir(tmp_path) if p.endswith(".tmp")]
    with open(path, encoding="utf-8") as handle:
        raw = json.load(handle)
    assert raw["records"]["email:1"]["status"] == "completed"

    # A fresh process sees the completed key.
    reopened = IdempotencyGuard(JsonFileStore(path))
    value, cached = reopened.call("email:1", lambda: {"sent": 999})
    assert cached is True
    assert value == {"sent": 3}


def test_json_file_store_survives_a_corrupt_file(tmp_path):
    path = str(tmp_path / "idem.json")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("{not json at all")
    store = JsonFileStore(path)
    assert store.keys() == []
    assert os.path.exists(path + ".corrupt"), "the bad file is kept, not deleted"


def test_non_serialisable_values_are_flagged_rather_than_faked(tmp_path):
    store = JsonFileStore(str(tmp_path / "idem.json"))
    guard = IdempotencyGuard(store)
    guard.call("obj", lambda: object())
    record = store.get("obj")
    assert record.status == "completed"
    assert record.value_available is False


def test_rerun_after_partial_failure_does_not_repeat_the_side_effect():
    """The whole point: run 1 emails, run 1 fails later, run 2 does not email."""
    emails: list = []
    ledger_up = {"value": False}

    def build() -> Workflow:
        workflow = Workflow("invoices")

        @workflow.task("batch")
        def batch(ctx: TaskContext) -> dict:
            return {"batch_id": "B-1", "customers": ["a@x", "b@x"]}

        @workflow.task(
            "email",
            depends_on=["batch"],
            idempotency_key=lambda ctx: content_key(
                "email", **ctx.result("batch")
            ),
        )
        def email(ctx: TaskContext) -> int:
            emails.extend(ctx.result("batch")["customers"])
            return len(emails)

        @workflow.task("ledger", depends_on=["email"])
        def ledger(ctx: TaskContext) -> str:
            if not ledger_up["value"]:
                raise TransientError("ledger down")
            return "posted"

        return workflow

    guard = IdempotencyGuard(MemoryStore())
    workflow = build()
    first = Executor(workflow, idempotency=guard).run()
    assert first.status.value == "failed"
    assert len(emails) == 2

    ledger_up["value"] = True
    second = Executor(workflow, idempotency=guard).run()
    assert second.status.value == "succeeded"
    assert second.tasks["email"].status is TaskStatus.CACHED
    assert len(emails) == 2, "the re-run must not email anybody a second time"
    assert second.tasks["email"].idempotency_key == first.tasks["email"].idempotency_key


def test_retries_inside_a_task_reuse_the_same_key():
    """A retry must not mint a new key, or the guard protects nothing."""
    seen = []
    workflow = Workflow("keyed_retry")

    @workflow.task(
        "call",
        retry=ExponentialBackoff(max_attempts=3, base=0.001),
        idempotency_key=lambda ctx: content_key("call", payload="fixed"),
    )
    def call(ctx: TaskContext) -> str:
        seen.append(ctx.attempt)
        if ctx.attempt < 3:
            raise TransientError("try again")
        return "ok"

    guard = IdempotencyGuard(MemoryStore())
    state = Executor(workflow, idempotency=guard).run()
    assert state.tasks["call"].attempts == 3
    assert seen == [1, 2, 3]
    assert len(guard.store.keys()) == 1


def test_guard_clock_is_injectable():
    clock = ManualClock()
    guard = IdempotencyGuard(MemoryStore(), clock=clock)
    guard.call("k", lambda: 1)
    record = guard.lookup("k")
    assert record.created_at.startswith("2026-01-01")
