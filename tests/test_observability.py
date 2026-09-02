"""Logs, timeline, Gantt render, metrics, and the failure explainer."""

from __future__ import annotations

import json

import pytest

from flowforge import Executor, TaskContext, Workflow
from flowforge.clock import ManualClock
from flowforge.observability import (
    ListSink,
    RunLogger,
    explain_failure,
    metrics,
    percentile,
    render_gantt,
    summarise,
    timeline,
)
from flowforge.state import RunState, RunStatus, TaskRecord, TaskStatus


def sample_state() -> RunState:
    """A fixed run: two successes, one failure, one blocked, one skipped."""
    state = RunState(
        run_id="r-42", workflow="nightly", status=RunStatus.FAILED, duration=1.0
    )
    state.tasks["extract"] = TaskRecord(
        "extract", TaskStatus.SUCCEEDED, attempts=1, start_offset=0.0, duration=0.2
    )
    state.tasks["transform"] = TaskRecord(
        "transform", TaskStatus.SUCCEEDED, attempts=3, start_offset=0.2, duration=0.4
    )
    state.tasks["publish"] = TaskRecord(
        "publish",
        TaskStatus.FAILED,
        attempts=2,
        start_offset=0.6,
        duration=0.4,
        error="connection refused",
        error_type="TransientError",
        retry_reasons=["retry:attempt=2", "attempts-exhausted:2"],
    )
    state.tasks["confirm"] = TaskRecord(
        "confirm", TaskStatus.UPSTREAM_FAILED, blocked_by="publish"
    )
    state.tasks["archive"] = TaskRecord("archive", TaskStatus.SKIPPED, blocked_by="publish")
    return state


def test_timeline_is_ordered_by_start_time():
    rows = timeline(sample_state())
    assert [row.task_id for row in rows] == ["extract", "transform", "publish"]
    assert rows[1].end == pytest.approx(0.6)


def test_gantt_render_is_exact():
    text = render_gantt(sample_state(), width=20)
    assert text.splitlines() == [
        "extract   |####                |   0.20s  succeeded",
        "transform |    ########        |   0.40s  succeeded x3",
        "publish   |            XXXXXXXX|   0.40s  failed x2",
        "total                              1.00s",
    ]


def test_gantt_bars_overlap_when_tasks_run_in_parallel():
    state = RunState(run_id="r", workflow="w", duration=1.0)
    state.tasks["a"] = TaskRecord("a", TaskStatus.SUCCEEDED, 1, start_offset=0.0, duration=1.0)
    state.tasks["b"] = TaskRecord("b", TaskStatus.SUCCEEDED, 1, start_offset=0.0, duration=1.0)
    lines = render_gantt(state, width=10).splitlines()
    assert lines[0].split("|")[1] == lines[1].split("|")[1] == "##########"


def test_gantt_handles_a_run_where_nothing_started():
    assert render_gantt(RunState(run_id="r", workflow="w")) == "(no tasks ran)"


def test_percentile_uses_nearest_rank():
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert percentile(values, 50) == 3.0
    assert percentile(values, 95) == 5.0
    assert percentile(values, 1) == 1.0
    assert percentile([], 50) == 0.0
    with pytest.raises(ValueError):
        percentile(values, 0)


def test_metrics_count_every_outcome():
    summary = metrics(sample_state())
    assert summary.tasks_total == 5
    assert summary.succeeded == 2
    assert summary.failed == 1
    assert summary.upstream_failed == 1
    assert summary.skipped == 1
    assert summary.success_rate == pytest.approx(2 / 5)
    assert summary.retries == 3  # transform 3 attempts, publish 2 attempts
    assert summary.attempts == 6
    assert summary.p50_duration == pytest.approx(0.4)
    assert summary.p95_duration == pytest.approx(0.4)
    assert summary.max_duration == pytest.approx(0.4)
    assert summary.task_time == pytest.approx(1.0)
    assert summary.wall_time == pytest.approx(1.0)
    assert summary.parallel_speedup == pytest.approx(1.0)
    assert summary.to_dict()["status"] == "failed"


def test_explain_failure_names_the_root_cause_and_what_it_blocked():
    text = explain_failure(sample_state())
    assert "root cause (1 task(s) raised)" in text
    assert "publish: TransientError: connection refused" in text
    assert "confirm (upstream_failed)" in text
    assert "archive (skipped)" in text
    assert "completed anyway (2): extract, transform" in text
    assert "retry decisions: retry:attempt=2, attempts-exhausted:2" in text


def test_explain_failure_on_a_clean_run():
    state = RunState(run_id="r", workflow="w", status=RunStatus.SUCCEEDED)
    state.tasks["a"] = TaskRecord("a", TaskStatus.SUCCEEDED)
    assert "every task succeeded" in explain_failure(state)


def test_summarise_is_one_line():
    line = summarise(sample_state())
    assert line.startswith("nightly r-42: failed")
    assert "\n" not in line


def test_every_log_line_carries_the_run_id_and_task_id():
    sink = ListSink()
    workflow = Workflow("logged")

    @workflow.task("step")
    def step(ctx: TaskContext) -> int:
        ctx.log("task.custom", note="hello")
        return 1

    Executor(workflow, logger=RunLogger(sink=sink, enabled=True)).run(run_id="r-7")
    records = sink.records()
    assert records, "the run should have logged something"
    assert all(record["run_id"] == "r-7" for record in records)
    task_records = [r for r in records if r["event"].startswith("task.")]
    assert task_records and all(r["task_id"] == "step" for r in task_records)
    assert any(r["event"] == "task.custom" and r["note"] == "hello" for r in task_records)
    # Every line is valid JSON on its own: one object per line, no framing.
    for line in sink.lines:
        assert isinstance(json.loads(line), dict)


def test_logger_binds_extra_fields_and_uses_the_injected_clock():
    sink = ListSink()
    logger = RunLogger("run-1", sink=sink, clock=ManualClock(), enabled=True)
    logger.bind(task_id="t", attempt=2).warn("task.attempt_failed", error="boom")
    record = sink.records()[0]
    assert record["level"] == "warning"
    assert record["task_id"] == "t"
    assert record["attempt"] == 2
    assert record["ts"].startswith("2026-01-01")


def test_metrics_from_a_real_run_match_the_state():
    workflow = Workflow("measured")

    @workflow.task("a")
    def a(ctx: TaskContext) -> int:
        return 1

    @workflow.task("b", depends_on=["a"])
    def b(ctx: TaskContext) -> int:
        return 2

    state = Executor(workflow).run()
    summary = metrics(state)
    assert summary.tasks_total == 2
    assert summary.success_rate == 1.0
    assert summary.retries == 0
    assert summary.wall_time > 0
    assert summary.slowest_task in {"a", "b"}
