"""Durable state and resume: do not repeat work that already succeeded."""

from __future__ import annotations

import json

import pytest

from flowforge import (
    ExecutionOptions,
    Executor,
    RunState,
    RunStatus,
    TaskContext,
    TaskStatus,
    Workflow,
)
from flowforge.errors import TransientError
from flowforge.state import ResultArchive, TaskRecord


def pipeline(side_effects: list, fail_publish: bool = True) -> Workflow:
    """extract -> transform -> publish -> confirm, with a switchable failure."""
    workflow = Workflow("pipeline")

    @workflow.task("extract")
    def extract(ctx: TaskContext) -> list:
        side_effects.append("extract")
        return [{"id": 1}, {"id": 2}]

    @workflow.task("transform", depends_on=["extract"])
    def transform(ctx: TaskContext) -> list:
        side_effects.append("transform")
        return [{"id": row["id"], "doubled": row["id"] * 2} for row in ctx.result("extract")]

    @workflow.task("publish", depends_on=["transform"])
    def publish(ctx: TaskContext) -> str:
        side_effects.append("publish")
        if ctx.param("fail_publish", False):
            raise TransientError("publish endpoint refused the connection")
        return "published"

    @workflow.task("confirm", depends_on=["publish", "transform"])
    def confirm(ctx: TaskContext) -> int:
        side_effects.append("confirm")
        return len(ctx.result("transform"))

    return workflow


def test_state_round_trips_through_json(tmp_path):
    state = RunState(run_id="r1", workflow="wf", params={"date": "2026-01-01"})
    record = state.record("a")
    record.status = TaskStatus.SUCCEEDED
    record.attempts = 2
    record.duration = 1.25
    record.output_digest = "abc123"
    path = str(tmp_path / "state.json")
    state.save(path)

    reloaded = RunState.load(path)
    assert reloaded.run_id == "r1"
    assert reloaded.params == {"date": "2026-01-01"}
    assert reloaded.tasks["a"].status is TaskStatus.SUCCEEDED
    assert reloaded.tasks["a"].attempts == 2
    assert reloaded.tasks["a"].duration == 1.25
    assert json.loads(state.to_json())["tasks"]["a"]["output_digest"] == "abc123"


def test_resume_plan_lists_only_unfinished_tasks():
    state = RunState(run_id="r", workflow="wf")
    state.record("a").status = TaskStatus.SUCCEEDED
    state.record("b").status = TaskStatus.CACHED
    state.record("c").status = TaskStatus.FAILED
    state.record("d").status = TaskStatus.UPSTREAM_FAILED
    assert state.completed_tasks() == ["a", "b"]
    assert state.resume_from(["a", "b", "c", "d", "e"]) == ["c", "d", "e"]


def test_crashed_run_resumes_from_the_right_task_and_repeats_nothing(store):
    effects: list = []
    workflow = pipeline(effects)
    executor = Executor(
        workflow, options=ExecutionOptions(max_workers=2), state_store=store
    )

    first = executor.run({"fail_publish": True}, run_id="run-1")
    assert first.status is RunStatus.FAILED
    assert effects == ["extract", "transform", "publish"]
    assert first.tasks["confirm"].status is TaskStatus.UPSTREAM_FAILED

    effects.clear()
    previous = store.load("run-1", "pipeline")
    second = executor.run({"fail_publish": False}, run_id="run-2", resume=previous)

    assert second.status is RunStatus.SUCCEEDED
    # Only the two unfinished tasks ran again.
    assert effects == ["publish", "confirm"]
    assert second.tasks["extract"].status is TaskStatus.SUCCEEDED
    assert second.tasks["extract"].attempts == 1, "inherited, not re-executed"
    assert second.resumed_from == ["run-1"]


def test_resumed_run_can_still_read_upstream_results(store):
    effects: list = []
    workflow = pipeline(effects)
    executor = Executor(workflow, state_store=store)
    executor.run({"fail_publish": True}, run_id="a1")
    second = executor.run(
        {"fail_publish": False}, run_id="a2", resume=store.load("a1", "pipeline")
    )
    archive = ResultArchive(store.archive_path(second))
    # `confirm` returned len(transform), proving it read the restored value.
    assert archive.get("confirm") == 2
    assert archive.get("transform") == [{"id": 1, "doubled": 2}, {"id": 2, "doubled": 4}]


def test_resume_without_a_state_store_says_why_the_value_is_missing():
    """No store means no archive, and a downstream task must be told, not fooled."""
    workflow = Workflow("no_store")

    @workflow.task("produce")
    def produce(ctx: TaskContext) -> str:
        return "value"

    @workflow.task("consume", depends_on=["produce"])
    def consume(ctx: TaskContext) -> str:
        return ctx.result("produce")

    previous = RunState(run_id="old", workflow="no_store")
    previous.record("produce").status = TaskStatus.SUCCEEDED

    state = Executor(workflow).run(resume=previous)
    assert state.tasks["consume"].status is TaskStatus.FAILED
    assert "not persisted" in state.tasks["consume"].error


def test_state_store_lists_and_finds_runs(store):
    effects: list = []
    workflow = pipeline(effects)
    executor = Executor(workflow, state_store=store)
    executor.run({"fail_publish": True}, run_id="r-1")
    executor.run({"fail_publish": False}, run_id="r-2")

    runs = store.list_runs("pipeline")
    assert {run.run_id for run in runs} == {"r-1", "r-2"}
    assert store.latest("pipeline").run_id in {"r-1", "r-2"}
    assert store.latest_failed("pipeline").run_id == "r-1"
    with pytest.raises(FileNotFoundError):
        store.load("no-such-run")


def test_result_archive_flags_values_it_cannot_persist(tmp_path):
    archive = ResultArchive(str(tmp_path / "results.json"))
    assert archive.put("ok", {"a": 1}) is True
    assert archive.put("bad", object()) is False
    assert archive.get("ok") == {"a": 1}
    assert archive.has("bad") is False
    with pytest.raises(KeyError):
        archive.get("bad")


def test_run_metadata_counts_attempts_and_retries():
    state = RunState(run_id="r", workflow="wf")
    state.tasks["a"] = TaskRecord("a", TaskStatus.SUCCEEDED, attempts=3)
    state.tasks["b"] = TaskRecord("b", TaskStatus.SUCCEEDED, attempts=1)
    assert state.total_attempts == 4
    assert state.retry_count == 2
    assert state.succeeded == ["a", "b"]
    assert state.is_complete() is False
