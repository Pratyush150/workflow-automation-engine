"""Partial-failure semantics.

This is the file that matters most. The claim being tested: when one task
fails, the tasks that depended on it are recorded as ``upstream_failed`` -- a
distinct status, not a silent skip -- and every branch that did not depend on
it still runs to completion.
"""

from __future__ import annotations

import threading

import pytest

from flowforge import (
    ExecutionOptions,
    Executor,
    ExponentialBackoff,
    OnFailure,
    RunStatus,
    TaskContext,
    TaskStatus,
    Workflow,
)
from flowforge.errors import PermanentError, TransientError
from flowforge.task import Task


def branching_workflow(fail_left: bool = True) -> Workflow:
    """Two independent branches from one root; the left one can be made to fail.

              +-- left_a --> left_b
      seed ---+
              +-- right_a --> right_b
    """
    workflow = Workflow("branches")

    @workflow.task("seed")
    def seed(ctx: TaskContext) -> int:
        return 1

    @workflow.task("left_a", depends_on=["seed"])
    def left_a(ctx: TaskContext) -> int:
        if fail_left:
            raise PermanentError("left branch is broken")
        return 2

    @workflow.task("left_b", depends_on=["left_a"])
    def left_b(ctx: TaskContext) -> int:
        return ctx.result("left_a") + 1

    @workflow.task("right_a", depends_on=["seed"])
    def right_a(ctx: TaskContext) -> int:
        return 10

    @workflow.task("right_b", depends_on=["right_a"])
    def right_b(ctx: TaskContext) -> int:
        return ctx.result("right_a") + 1

    return workflow


@pytest.mark.parametrize("workers", [1, 4])
def test_failure_marks_downstream_upstream_failed_and_spares_other_branches(workers):
    workflow = branching_workflow(fail_left=True)
    state = Executor(workflow, options=ExecutionOptions(max_workers=workers)).run()

    assert state.status is RunStatus.FAILED
    assert state.tasks["left_a"].status is TaskStatus.FAILED
    # Not "skipped": it never got the chance to run.
    assert state.tasks["left_b"].status is TaskStatus.UPSTREAM_FAILED
    assert state.tasks["left_b"].blocked_by == "left_a"
    assert state.tasks["left_b"].attempts == 0
    # The independent branch is untouched.
    assert state.tasks["right_a"].status is TaskStatus.SUCCEEDED
    assert state.tasks["right_b"].status is TaskStatus.SUCCEEDED
    assert state.succeeded == ["right_a", "right_b", "seed"]
    assert state.tasks["left_a"].error_type == "PermanentError"
    assert "left branch is broken" in state.tasks["left_a"].error


def test_upstream_failure_propagates_through_a_long_chain():
    workflow = Workflow("chain")

    @workflow.task("one")
    def one(ctx: TaskContext) -> None:
        raise TransientError("down")

    previous = "one"
    for name in ("two", "three", "four"):
        workflow.add(
            workflow["one"].with_(id=name, depends_on=(previous,), fn=lambda ctx: 1)
        )
        previous = name

    state = Executor(workflow).run()
    assert state.tasks["one"].status is TaskStatus.FAILED
    for name in ("two", "three", "four"):
        assert state.tasks[name].status is TaskStatus.UPSTREAM_FAILED
    assert state.tasks["four"].blocked_by == "three"


def test_on_failure_skip_keeps_the_run_alive_but_degraded():
    workflow = Workflow("skip_policy")

    @workflow.task("core")
    def core(ctx: TaskContext) -> str:
        return "report"

    @workflow.task("push_metrics", depends_on=["core"], on_failure=OnFailure.SKIP)
    def push_metrics(ctx: TaskContext) -> None:
        raise TransientError("metrics gateway down")

    @workflow.task("update_dashboard", depends_on=["push_metrics"])
    def update_dashboard(ctx: TaskContext) -> str:
        return "updated"

    state = Executor(workflow).run()
    assert state.status is RunStatus.DEGRADED
    assert state.tasks["core"].status is TaskStatus.SUCCEEDED
    assert state.tasks["push_metrics"].status is TaskStatus.FAILED
    # Skipped, not upstream_failed: a policy chose this, nothing broke downstream.
    assert state.tasks["update_dashboard"].status is TaskStatus.SKIPPED


def test_on_failure_continue_runs_downstream_with_a_missing_input():
    workflow = Workflow("continue_policy")

    @workflow.task("enrich", on_failure=OnFailure.CONTINUE)
    def enrich(ctx: TaskContext) -> None:
        raise PermanentError("404 from the enrichment API")

    @workflow.task("report", depends_on=["enrich"])
    def report(ctx: TaskContext) -> str:
        return f"enrichment={ctx.result('enrich')}"

    state = Executor(workflow).run()
    assert state.status is RunStatus.DEGRADED
    assert state.tasks["enrich"].status is TaskStatus.FAILED
    assert state.tasks["report"].status is TaskStatus.SUCCEEDED


def test_fail_fast_cancels_work_that_has_not_started():
    workflow = branching_workflow(fail_left=True)
    state = Executor(
        workflow, options=ExecutionOptions(max_workers=1, fail_fast=True)
    ).run()
    assert state.status is RunStatus.FAILED
    statuses = {name: record.status for name, record in state.tasks.items()}
    assert statuses["left_a"] is TaskStatus.FAILED
    assert TaskStatus.CANCELLED in statuses.values()


def test_successful_run_reports_every_task():
    workflow = branching_workflow(fail_left=False)
    state = Executor(workflow, options=ExecutionOptions(max_workers=2)).run()
    assert state.status is RunStatus.SUCCEEDED
    assert len(state.succeeded) == 5
    assert state.retry_count == 0
    assert all(record.output_digest for record in state.tasks.values())


def test_retries_are_recorded_per_task():
    workflow = Workflow("retrying")
    attempts = {"n": 0}

    @workflow.task("flaky", retry=ExponentialBackoff(max_attempts=3, base=0.001))
    def flaky(ctx: TaskContext) -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise TransientError(f"attempt {attempts['n']}")
        return "ok"

    state = Executor(workflow).run()
    assert state.status is RunStatus.SUCCEEDED
    assert state.tasks["flaky"].attempts == 3
    assert state.retry_count == 2
    assert state.tasks["flaky"].retry_reasons[-1] == "ok"


def test_tasks_only_see_declared_dependencies():
    workflow = Workflow("isolation")

    @workflow.task("a")
    def a(ctx: TaskContext) -> str:
        return "value-a"

    @workflow.task("b")
    def b(ctx: TaskContext) -> str:
        return "value-b"

    @workflow.task("c", depends_on=["a"])
    def c(ctx: TaskContext) -> str:
        with pytest.raises(PermanentError) as excinfo:
            ctx.result("b")
        assert "depends_on" in str(excinfo.value)
        return ctx.result("a")

    state = Executor(workflow, options=ExecutionOptions(max_workers=1)).run()
    assert state.status is RunStatus.SUCCEEDED


def test_parallel_execution_actually_overlaps():
    workflow = Workflow("parallel")
    barrier = threading.Barrier(3, timeout=5.0)

    def wait_together(ctx: TaskContext) -> str:
        # Only passes if all three run at the same time.
        barrier.wait()
        return ctx.task_id

    for name in ("p1", "p2", "p3"):
        workflow.add(Task(id=name, fn=wait_together))

    state = Executor(workflow, options=ExecutionOptions(max_workers=3)).run()
    assert state.status is RunStatus.SUCCEEDED
    assert barrier.broken is False


def test_sequential_execution_respects_declaration_order():
    workflow = Workflow("ordered")
    order = []

    @workflow.task("first")
    def first(ctx: TaskContext) -> None:
        order.append("first")

    @workflow.task("second", depends_on=["first"])
    def second(ctx: TaskContext) -> None:
        order.append("second")

    @workflow.task("third", depends_on=["second"])
    def third(ctx: TaskContext) -> None:
        order.append("third")

    Executor(workflow, options=ExecutionOptions(max_workers=1)).run()
    assert order == ["first", "second", "third"]


def test_run_state_is_persisted_for_every_transition(store):
    workflow = branching_workflow(fail_left=True)
    state = Executor(workflow, state_store=store).run()
    reloaded = store.load(state.run_id, "branches")
    assert reloaded.status is RunStatus.FAILED
    assert reloaded.tasks["left_b"].status is TaskStatus.UPSTREAM_FAILED
    assert reloaded.tasks["right_b"].status is TaskStatus.SUCCEEDED
