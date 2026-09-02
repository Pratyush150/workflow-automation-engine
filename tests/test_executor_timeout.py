"""Timeouts, deadlines and cancellation.

The real sleeps in this file are tens of milliseconds. They are real on purpose:
a timeout that only works against a fake clock is not a timeout.
"""

from __future__ import annotations

import time

from flowforge import (
    ExecutionOptions,
    Executor,
    RunStatus,
    TaskContext,
    TaskStatus,
    Workflow,
)
from flowforge.observability import ListSink, RunLogger


def test_timeout_fires_and_marks_the_task_timed_out():
    workflow = Workflow("timeouts")

    @workflow.task("slow", timeout=0.05)
    def slow(ctx: TaskContext) -> str:
        time.sleep(0.4)
        return "should never be used"

    @workflow.task("after", depends_on=["slow"])
    def after(ctx: TaskContext) -> str:
        return "ran"

    started = time.monotonic()
    state = Executor(workflow).run()
    elapsed = time.monotonic() - started

    assert state.tasks["slow"].status is TaskStatus.TIMED_OUT
    assert "timeout of 0.05s" in state.tasks["slow"].error
    assert state.tasks["after"].status is TaskStatus.UPSTREAM_FAILED
    assert state.status is RunStatus.FAILED
    # The engine stopped waiting well before the task finished sleeping.
    assert elapsed < 0.35


def test_timeout_sets_the_cancel_token_so_a_cooperative_task_stops():
    workflow = Workflow("cooperative")
    observed = {"cancelled": False, "loops": 0}

    @workflow.task("polling", timeout=0.05)
    def polling(ctx: TaskContext) -> str:
        for _ in range(200):
            if ctx.cancel.cancelled:
                observed["cancelled"] = True
                observed["loops"] += 1
                raise SystemExit  # pragma: no cover - never reached in practice
            observed["loops"] += 1
            time.sleep(0.005)
        return "finished"

    state = Executor(workflow).run()
    assert state.tasks["polling"].status is TaskStatus.TIMED_OUT
    # Give the abandoned thread a moment to notice the token.
    deadline = time.monotonic() + 1.0
    while not observed["cancelled"] and time.monotonic() < deadline:
        time.sleep(0.005)
    assert observed["cancelled"] is True


def test_late_result_from_a_timed_out_task_is_ignored():
    """A task that finishes after we gave up must not resurrect itself."""
    workflow = Workflow("late")
    sink = ListSink()

    @workflow.task("slow", timeout=0.05)
    def slow(ctx: TaskContext) -> str:
        time.sleep(0.12)
        return "late"

    @workflow.task("other")
    def other(ctx: TaskContext) -> str:
        time.sleep(0.25)
        return "on time"

    state = Executor(
        workflow,
        options=ExecutionOptions(max_workers=2),
        logger=RunLogger(sink=sink, enabled=True),
    ).run()
    assert state.tasks["slow"].status is TaskStatus.TIMED_OUT
    assert state.tasks["other"].status is TaskStatus.SUCCEEDED
    events = [record["event"] for record in sink.records()]
    assert "task.timeout" in events
    assert "task.late_result_ignored" in events


def test_run_deadline_cancels_everything_still_outstanding():
    workflow = Workflow("deadline")

    @workflow.task("one")
    def one(ctx: TaskContext) -> str:
        time.sleep(0.2)
        return "one"

    @workflow.task("two", depends_on=["one"])
    def two(ctx: TaskContext) -> str:
        return "two"

    state = Executor(workflow, options=ExecutionOptions(deadline=0.05)).run()
    assert state.status is RunStatus.CANCELLED
    assert "deadline" in state.error
    assert state.tasks["two"].status is TaskStatus.CANCELLED


def test_default_timeout_applies_to_tasks_without_one():
    workflow = Workflow("default_timeout")

    @workflow.task("slow")
    def slow(ctx: TaskContext) -> str:
        time.sleep(0.4)
        return "nope"

    state = Executor(workflow, options=ExecutionOptions(default_timeout=0.05)).run()
    assert state.tasks["slow"].status is TaskStatus.TIMED_OUT


def test_queued_task_is_not_charged_for_time_it_spent_waiting():
    """A task's timeout starts when it starts, not when it was submitted."""
    workflow = Workflow("queueing")

    @workflow.task("hog")
    def hog(ctx: TaskContext) -> str:
        time.sleep(0.15)
        return "hog"

    @workflow.task("quick", timeout=0.1)
    def quick(ctx: TaskContext) -> str:
        return "quick"

    # One worker: `quick` sits in the queue behind `hog` for longer than its
    # own timeout, then runs instantly. It must succeed.
    state = Executor(workflow, options=ExecutionOptions(max_workers=1)).run()
    assert state.tasks["quick"].status is TaskStatus.SUCCEEDED
    assert state.status is RunStatus.SUCCEEDED
