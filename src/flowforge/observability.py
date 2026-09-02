"""Structured logs, a run timeline, an ASCII Gantt chart, metrics, and a
root-cause explainer.

Automation fails quietly. A workflow that half-worked at 03:00 and printed
nothing anybody read is indistinguishable from one that worked, right up until
somebody asks where the numbers went. Everything in this module exists to make
a run answer three questions without a debugger:

1. What happened, in order, with a run id and task id on **every** line.
2. Where did the time go? (:func:`render_gantt`)
3. What actually broke, and what did it take down with it?
   (:func:`explain_failure`)
"""

from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence

from .clock import SYSTEM_CLOCK, Clock
from .state import RunState, RunStatus, TaskStatus

__all__ = [
    "RunLogger",
    "RunMetrics",
    "TimelineEntry",
    "explain_failure",
    "metrics",
    "percentile",
    "ListSink",
    "render_gantt",
    "summarise",
    "timeline",
]


class RunLogger:
    """JSON-lines logger that always carries the run id.

    One JSON object per line, so ``jq`` and every log shipper on earth can read
    it without a custom parser. ``bind`` returns a child logger with extra
    fields merged in -- the executor binds ``task_id`` and ``attempt`` so a
    line is attributable even when eight tasks are interleaved across a pool.

    ``sink`` takes the rendered line. The default writes to stderr, keeping
    stdout free for a workflow that produces real output.
    """

    def __init__(
        self,
        run_id: str = "",
        *,
        sink: Optional[Callable[[str], None]] = None,
        clock: Clock = SYSTEM_CLOCK,
        fields: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
    ) -> None:
        self.run_id = run_id
        self.clock = clock
        self.enabled = enabled
        self._fields = dict(fields or {})
        self._sink = sink if sink is not None else _stderr_sink

    def bind(self, **fields: Any) -> "RunLogger":
        child = RunLogger(
            self.run_id,
            sink=self._sink,
            clock=self.clock,
            fields={**self._fields, **fields},
            enabled=self.enabled,
        )
        return child

    def event(self, event: str, level: str = "info", **fields: Any) -> Dict[str, Any]:
        """Emit one structured event. Returns the record, which tests assert on."""
        record: Dict[str, Any] = {
            "ts": self.clock.now().isoformat(),
            "level": level,
            "event": event,
            "run_id": self.run_id,
        }
        record.update(self._fields)
        record.update(fields)
        if self.enabled:
            self._sink(json.dumps(record, sort_keys=False, default=str))
        return record

    # Convenience wrappers so call sites read like logging, not like plumbing.
    def info(self, event: str, **fields: Any) -> Dict[str, Any]:
        return self.event(event, "info", **fields)

    def warn(self, event: str, **fields: Any) -> Dict[str, Any]:
        return self.event(event, "warning", **fields)

    def error(self, event: str, **fields: Any) -> Dict[str, Any]:
        return self.event(event, "error", **fields)

    def __call__(self, event: str, **fields: Any) -> Dict[str, Any]:
        return self.event(event, **fields)


def _stderr_sink(line: str) -> None:
    print(line, file=sys.stderr)


class ListSink:
    """Collect log lines in memory. Used by tests and by the CLI's ``--quiet``."""

    def __init__(self) -> None:
        self.lines: List[str] = []

    def __call__(self, line: str) -> None:
        self.lines.append(line)

    def records(self) -> List[Dict[str, Any]]:
        return [json.loads(line) for line in self.lines]

    def events(self, name: str) -> List[Dict[str, Any]]:
        return [r for r in self.records() if r.get("event") == name]


@dataclass
class TimelineEntry:
    """One row of the run timeline."""

    task_id: str
    status: TaskStatus
    start: float
    duration: float
    attempts: int

    @property
    def end(self) -> float:
        return self.start + self.duration


def timeline(state: RunState) -> List[TimelineEntry]:
    """Rows for every task that started, ordered by start time then id."""
    rows = [
        TimelineEntry(
            task_id=rec.task_id,
            status=rec.status,
            start=rec.start_offset or 0.0,
            duration=rec.duration or 0.0,
            attempts=rec.attempts,
        )
        for rec in state.tasks.values()
        if rec.start_offset is not None
    ]
    return sorted(rows, key=lambda r: (r.start, r.task_id))


#: Single characters per status, so a run reads at a glance.
STATUS_GLYPH: Dict[TaskStatus, str] = {
    TaskStatus.SUCCEEDED: "#",
    TaskStatus.CACHED: "=",
    TaskStatus.FAILED: "X",
    TaskStatus.TIMED_OUT: "T",
    TaskStatus.UPSTREAM_FAILED: "!",
    TaskStatus.SKIPPED: "~",
    TaskStatus.CANCELLED: "-",
    TaskStatus.RUNNING: ">",
    TaskStatus.PENDING: ".",
}


def render_gantt(state: RunState, width: int = 40, name_width: int = 0) -> str:
    """ASCII Gantt chart of the run.

    Parallel tasks visibly overlap, which is the fastest way to see that the
    "parallel" workflow you wrote is in fact a chain. The bar character encodes
    status, so a failed task is obvious without colour::

        extract   |####----------------|  0.20s  succeeded
        transform |    ################|  0.60s  succeeded

    Widths are fixed and derived only from the state, so the output is stable
    and can be asserted on in a test.
    """
    rows = timeline(state)
    if not rows:
        return "(no tasks ran)"
    # Scale against the whole run, not just the last task to finish, so a run
    # with idle time at the end does not silently stretch the bars.
    span = max([r.end for r in rows] + [state.duration or 0.0])
    if span <= 0:
        span = 1e-9
    name_width = name_width or max(len(r.task_id) for r in rows)
    lines: List[str] = []
    for row in rows:
        # Round before flooring/ceiling: 0.2 + 0.4 is 0.6000000000000001 in
        # binary floating point, which would otherwise widen the bar by one
        # character and make the render unstable across machines.
        start_col = int(round(row.start / span * width, 6))
        end_col = int(math.ceil(round(row.end / span * width, 6)))
        start_col = min(start_col, width - 1)
        end_col = max(end_col, start_col + 1)
        end_col = min(end_col, width)
        glyph = STATUS_GLYPH.get(row.status, "?")
        bar = (
            " " * start_col
            + glyph * (end_col - start_col)
            + " " * (width - end_col)
        )
        attempts = f" x{row.attempts}" if row.attempts > 1 else ""
        lines.append(
            f"{row.task_id.ljust(name_width)} |{bar}| "
            f"{row.duration:6.2f}s  {row.status.value}{attempts}"
        )
    total = state.duration if state.duration is not None else span
    lines.append(f"{'total'.ljust(name_width)}  {' ' * width}  {total:6.2f}s")
    return "\n".join(lines)


def percentile(values: Sequence[float], pct: float) -> float:
    """Nearest-rank percentile.

    No interpolation. With five samples a p95 that interpolates invents a number
    that no task actually took; nearest-rank returns a real observation, which
    is what you want when you are about to argue about a timeout value.
    """
    if not values:
        return 0.0
    if not 0 < pct <= 100:
        raise ValueError("pct must be in (0, 100]")
    ordered = sorted(values)
    rank = math.ceil(pct / 100.0 * len(ordered))
    return ordered[max(1, rank) - 1]


@dataclass
class RunMetrics:
    """Aggregates for one run."""

    run_id: str = ""
    workflow: str = ""
    status: RunStatus = RunStatus.PENDING
    tasks_total: int = 0
    succeeded: int = 0
    cached: int = 0
    failed: int = 0
    timed_out: int = 0
    upstream_failed: int = 0
    skipped: int = 0
    cancelled: int = 0
    retries: int = 0
    attempts: int = 0
    success_rate: float = 0.0
    p50_duration: float = 0.0
    p95_duration: float = 0.0
    max_duration: float = 0.0
    slowest_task: str = ""
    wall_time: float = 0.0
    task_time: float = 0.0

    @property
    def parallel_speedup(self) -> float:
        """Sum of task time over wall time. 1.0 means fully sequential."""
        if self.wall_time <= 0:
            return 0.0
        return self.task_time / self.wall_time

    def to_dict(self) -> Dict[str, Any]:
        data = {k: v for k, v in self.__dict__.items()}
        data["status"] = self.status.value
        data["parallel_speedup"] = round(self.parallel_speedup, 3)
        return data


def metrics(state: RunState) -> RunMetrics:
    """Compute :class:`RunMetrics` from a run state.

    ``success_rate`` counts tasks that ran and succeeded (including cache hits)
    over all tasks in the run -- tasks blocked by an upstream failure count
    against it, because from an operator's point of view they did not happen.
    """
    durations = [
        r.duration
        for r in state.tasks.values()
        if r.duration is not None and r.status.is_terminal and r.attempts > 0
    ]
    counts = {status: 0 for status in TaskStatus}
    for rec in state.tasks.values():
        counts[rec.status] += 1
    total = len(state.tasks)
    ok = counts[TaskStatus.SUCCEEDED] + counts[TaskStatus.CACHED]
    slowest = ""
    if durations:
        slowest = max(
            (r for r in state.tasks.values() if r.duration is not None),
            key=lambda r: (r.duration or 0.0, r.task_id),
        ).task_id
    return RunMetrics(
        run_id=state.run_id,
        workflow=state.workflow,
        status=state.status,
        tasks_total=total,
        succeeded=counts[TaskStatus.SUCCEEDED],
        cached=counts[TaskStatus.CACHED],
        failed=counts[TaskStatus.FAILED],
        timed_out=counts[TaskStatus.TIMED_OUT],
        upstream_failed=counts[TaskStatus.UPSTREAM_FAILED],
        skipped=counts[TaskStatus.SKIPPED],
        cancelled=counts[TaskStatus.CANCELLED],
        retries=state.retry_count,
        attempts=state.total_attempts,
        success_rate=(ok / total) if total else 0.0,
        p50_duration=percentile(durations, 50),
        p95_duration=percentile(durations, 95),
        max_duration=max(durations, default=0.0),
        slowest_task=slowest,
        wall_time=state.duration or 0.0,
        task_time=sum(durations),
    )


def explain_failure(state: RunState, dag: Optional[Any] = None) -> str:
    """Walk the state and explain the failure the way a person would.

    Names the root-cause tasks -- the ones that actually raised -- then lists
    everything that never ran because of them. The distinction matters when a
    dashboard shows eleven red tasks: ten of them are noise, one is the
    incident.
    """
    lines: List[str] = []
    roots = [
        rec
        for rec in sorted(state.tasks.values(), key=lambda r: r.task_id)
        if rec.status.is_failure
    ]
    blocked = [
        rec
        for rec in sorted(state.tasks.values(), key=lambda r: r.task_id)
        if rec.status in (TaskStatus.UPSTREAM_FAILED, TaskStatus.SKIPPED, TaskStatus.CANCELLED)
    ]
    header = f"run {state.run_id} of workflow {state.workflow!r}: {state.status.value}"
    lines.append(header)
    if not roots:
        if state.status is RunStatus.SUCCEEDED:
            lines.append("no failures: every task succeeded")
        else:
            lines.append("no task raised; check the run-level error")
            if state.error:
                lines.append(f"  run error: {state.error}")
        if blocked:
            lines.append("")
            lines.append("did not run:")
            for rec in blocked:
                lines.append(f"  {rec.task_id} ({rec.status.value})")
        return "\n".join(lines)

    lines.append("")
    lines.append(f"root cause ({len(roots)} task(s) raised):")
    for rec in roots:
        lines.append(
            f"  {rec.task_id}: {rec.error_type or 'error'}: {rec.error or '(no message)'}"
        )
        lines.append(
            f"    attempts={rec.attempts} duration={_fmt(rec.duration)} "
            f"status={rec.status.value}"
        )
        if rec.retry_reasons:
            lines.append(f"    retry decisions: {', '.join(rec.retry_reasons)}")
        downstream = _blocked_by(state, rec.task_id, dag)
        if downstream:
            lines.append(f"    blocked {len(downstream)} downstream task(s):")
            for task_id in downstream:
                lines.append(
                    f"      {task_id} ({state.tasks[task_id].status.value})"
                )
        else:
            lines.append("    blocked nothing downstream")

    unaffected = sorted(t for t, r in state.tasks.items() if r.status.is_success)
    if unaffected:
        lines.append("")
        lines.append(
            f"completed anyway ({len(unaffected)}): {', '.join(unaffected)}"
        )
    orphan_blocked = [
        rec.task_id
        for rec in blocked
        if not rec.blocked_by
        or rec.blocked_by not in {r.task_id for r in roots}
    ]
    if orphan_blocked:
        lines.append("")
        lines.append(
            "also did not run (blocked further down the chain): "
            + ", ".join(sorted(orphan_blocked))
        )
    return "\n".join(lines)


def _blocked_by(state: RunState, task_id: str, dag: Optional[Any]) -> List[str]:
    """Tasks that did not run because of ``task_id``."""
    direct = sorted(
        t
        for t, rec in state.tasks.items()
        if rec.blocked_by == task_id
        and rec.status in (TaskStatus.UPSTREAM_FAILED, TaskStatus.SKIPPED)
    )
    if dag is not None:
        try:
            descendants = dag.descendants(task_id)
        except Exception:  # pragma: no cover - defensive: dag may not know the id
            descendants = set()
        direct = sorted(
            set(direct)
            | {
                t
                for t in descendants
                if state.status_of(t)
                in (TaskStatus.UPSTREAM_FAILED, TaskStatus.SKIPPED)
            }
        )
    return direct


def _fmt(value: Optional[float]) -> str:
    return "n/a" if value is None else f"{value:.3f}s"


def summarise(state: RunState) -> str:
    """One-line human summary, used by the CLI."""
    m = metrics(state)
    return (
        f"{state.workflow} {state.run_id}: {state.status.value} "
        f"({m.succeeded} ok, {m.cached} cached, {m.failed + m.timed_out} failed, "
        f"{m.upstream_failed} upstream_failed, {m.skipped} skipped) "
        f"in {m.wall_time:.2f}s, {m.retries} retries"
    )
