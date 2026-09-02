"""Durable run state.

A run that cannot be inspected after it dies is a run you cannot operate. This
module records, per task: status, attempt count, start and end timestamps,
duration, a digest of the inputs it saw and the output it produced, the error
type and message, and the idempotency key it used.

The state is written as JSON after every task transition, so the answer to
"where did last night's run get to?" is a file, not a guess -- and
:meth:`RunState.resume_from` turns that file into a plan that re-runs only what
did not already succeed.

Digests, not payloads. Task outputs can be large or contain credentials; the
state file stores a 16-hex-char content hash. Enough to prove two runs saw the
same input, not enough to leak a customer list into a log directory.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

from .idempotency import digest

__all__ = ["RunState", "RunStatus", "StateStore", "TaskRecord", "TaskStatus"]


class TaskStatus(str, Enum):
    """Per-task status.

    ``UPSTREAM_FAILED`` and ``SKIPPED`` are separate on purpose. "Did not run
    because something it needed broke" and "did not run because we chose not to
    run it" are different incidents, and collapsing them into one "skipped"
    bucket is how partial failures go unnoticed for a week.
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    CACHED = "cached"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self is not TaskStatus.PENDING and self is not TaskStatus.RUNNING

    @property
    def is_success(self) -> bool:
        return self in (TaskStatus.SUCCEEDED, TaskStatus.CACHED)

    @property
    def is_failure(self) -> bool:
        return self in (TaskStatus.FAILED, TaskStatus.TIMED_OUT)


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    #: Finished, but something failed under a non-fatal ``on_failure`` policy.
    DEGRADED = "degraded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskRecord:
    """Everything known about one task in one run."""

    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    #: Monotonic offsets from run start, in seconds. Used by the Gantt render.
    start_offset: Optional[float] = None
    duration: Optional[float] = None
    input_digest: str = ""
    output_digest: str = ""
    error: str = ""
    error_type: str = ""
    idempotency_key: str = ""
    blocked_by: str = ""
    retry_reasons: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskRecord":
        known = set(cls.__dataclass_fields__)  # type: ignore[attr-defined]
        clean = {k: v for k, v in data.items() if k in known}
        clean["status"] = TaskStatus(clean.get("status", "pending"))
        return cls(**clean)


@dataclass
class RunState:
    """The durable record of one workflow run."""

    run_id: str
    workflow: str
    status: RunStatus = RunStatus.PENDING
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    duration: Optional[float] = None
    params: Dict[str, Any] = field(default_factory=dict)
    tasks: Dict[str, TaskRecord] = field(default_factory=dict)
    error: str = ""
    #: Run ids this run resumed from, oldest first.
    resumed_from: List[str] = field(default_factory=list)
    version: int = 1

    # ------------------------------------------------------------------ access

    def record(self, task_id: str) -> TaskRecord:
        """Get or create the record for ``task_id``."""
        if task_id not in self.tasks:
            self.tasks[task_id] = TaskRecord(task_id=task_id)
        return self.tasks[task_id]

    def status_of(self, task_id: str) -> TaskStatus:
        rec = self.tasks.get(task_id)
        return rec.status if rec else TaskStatus.PENDING

    def by_status(self, *statuses: TaskStatus) -> List[str]:
        wanted = set(statuses)
        return sorted(t for t, r in self.tasks.items() if r.status in wanted)

    @property
    def succeeded(self) -> List[str]:
        return sorted(t for t, r in self.tasks.items() if r.status.is_success)

    @property
    def failed(self) -> List[str]:
        return sorted(t for t, r in self.tasks.items() if r.status.is_failure)

    @property
    def total_attempts(self) -> int:
        return sum(r.attempts for r in self.tasks.values())

    @property
    def retry_count(self) -> int:
        """Attempts beyond the first, across all tasks."""
        return sum(max(0, r.attempts - 1) for r in self.tasks.values())

    def is_complete(self) -> bool:
        return self.status in (
            RunStatus.SUCCEEDED,
            RunStatus.DEGRADED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
        )

    # ------------------------------------------------------------------ resume

    def completed_tasks(self) -> List[str]:
        """Tasks that must not run again on a resume."""
        return sorted(t for t, r in self.tasks.items() if r.status.is_success)

    def resume_from(self, all_task_ids: Iterable[str]) -> List[str]:
        """Task ids a resume should attempt.

        Everything that did not reach a success state, plus anything in the
        workflow that this run never reached at all. A task that succeeded and
        one that was served from the idempotency cache both count as done.
        """
        done = set(self.completed_tasks())
        return sorted(t for t in all_task_ids if t not in done)

    def as_resume_seed(self, new_run_id: str) -> "RunState":
        """A fresh run state that inherits this run's successful tasks."""
        seed = RunState(
            run_id=new_run_id,
            workflow=self.workflow,
            params=dict(self.params),
            resumed_from=self.resumed_from + [self.run_id],
        )
        for task_id in self.completed_tasks():
            old = self.tasks[task_id]
            seed.tasks[task_id] = TaskRecord.from_dict(old.to_dict())
        return seed

    # -------------------------------------------------------------- (de)serial

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "run_id": self.run_id,
            "workflow": self.workflow,
            "status": self.status.value,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration": self.duration,
            "params": self.params,
            "error": self.error,
            "resumed_from": list(self.resumed_from),
            "tasks": {k: v.to_dict() for k, v in self.tasks.items()},
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True, default=str)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunState":
        state = cls(
            run_id=data["run_id"],
            workflow=data.get("workflow", ""),
            status=RunStatus(data.get("status", "pending")),
            started_at=data.get("started_at"),
            ended_at=data.get("ended_at"),
            duration=data.get("duration"),
            params=dict(data.get("params", {})),
            error=data.get("error", ""),
            resumed_from=list(data.get("resumed_from", [])),
            version=int(data.get("version", 1)),
        )
        for task_id, raw in data.get("tasks", {}).items():
            state.tasks[task_id] = TaskRecord.from_dict(raw)
        return state

    @classmethod
    def from_json(cls, text: str) -> "RunState":
        return cls.from_dict(json.loads(text))

    def save(self, path: str) -> None:
        """Atomically write this state to ``path``."""
        _atomic_write(path, self.to_json())

    @classmethod
    def load(cls, path: str) -> "RunState":
        with open(path, "r", encoding="utf-8") as fh:
            return cls.from_json(fh.read())


def _atomic_write(path: str, text: str) -> None:
    parent = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(parent, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=parent, prefix=".ff-state-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


class StateStore:
    """A directory of run-state JSON files.

    One file per run, named ``<workflow>.<run_id>.json``. Deliberately boring:
    you can read it with ``cat``, diff two runs, and grep a week of them for a
    task id without any tooling at all.
    """

    def __init__(self, directory: str = ".flowforge/runs") -> None:
        self.directory = str(directory)

    def _path(self, state_or_id: Any, workflow: str = "") -> str:
        if isinstance(state_or_id, RunState):
            return os.path.join(
                self.directory, f"{state_or_id.workflow}.{state_or_id.run_id}.json"
            )
        return os.path.join(self.directory, f"{workflow}.{state_or_id}.json")

    def save(self, state: RunState) -> str:
        path = self._path(state)
        state.save(path)
        return path

    def load(self, run_id: str, workflow: str = "") -> RunState:
        if workflow:
            return RunState.load(self._path(run_id, workflow))
        for path in self._all_paths():
            if os.path.basename(path).endswith(f".{run_id}.json"):
                return RunState.load(path)
        raise FileNotFoundError(f"no run state for run_id {run_id!r} in {self.directory}")

    def _all_paths(self) -> List[str]:
        if not os.path.isdir(self.directory):
            return []
        return sorted(
            os.path.join(self.directory, name)
            for name in os.listdir(self.directory)
            if name.endswith(".json")
        )

    def list_runs(self, workflow: Optional[str] = None) -> List[RunState]:
        """All stored runs, newest first by start time."""
        out: List[RunState] = []
        for path in self._all_paths():
            try:
                state = RunState.load(path)
            except (json.JSONDecodeError, OSError, KeyError):
                continue
            if workflow and state.workflow != workflow:
                continue
            out.append(state)
        return sorted(out, key=lambda s: (s.started_at or "", s.run_id), reverse=True)

    def latest(self, workflow: Optional[str] = None) -> Optional[RunState]:
        runs = self.list_runs(workflow)
        return runs[0] if runs else None

    def archive_path(self, state: "RunState") -> str:
        """Path of the result archive that belongs to ``state``."""
        return os.path.join(
            self.directory, f"{state.workflow}.{state.run_id}.results.json"
        )

    def latest_failed(self, workflow: Optional[str] = None) -> Optional[RunState]:
        for state in self.list_runs(workflow):
            if state.status in (RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.DEGRADED):
                return state
        return None


def value_digest(value: Any) -> str:
    """Digest helper re-exported so callers do not import idempotency for it."""
    return digest(value)


class ResultArchive:
    """JSON sidecar holding task outputs so a resumed run can read them.

    The run state stores digests only. That is right for auditing and wrong for
    resuming: a resumed run still has to hand ``ctx.result("extract")`` to the
    transform task that did not get to run. This archive keeps the actual
    values next to the state file.

    Values that do not survive a JSON round trip are recorded as unavailable
    rather than silently dropped, so the resumed run fails with "task X's output
    was not persistable" instead of quietly processing ``None``.
    """

    def __init__(self, path: str) -> None:
        self.path = str(path)
        self._data: Dict[str, Dict[str, Any]] = {}
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as fh:
                    self._data = json.load(fh).get("results", {})
            except (json.JSONDecodeError, OSError):
                self._data = {}

    def put(self, task_id: str, value: Any) -> bool:
        """Store ``value``. Returns False if it could not be serialised."""
        try:
            json.dumps(value)
        except (TypeError, ValueError):
            self._data[task_id] = {"available": False, "type": type(value).__name__}
            self._save()
            return False
        self._data[task_id] = {
            "available": True,
            "value": value,
            "digest": digest(value),
        }
        self._save()
        return True

    def get(self, task_id: str) -> Any:
        entry = self._data.get(task_id)
        if not entry or not entry.get("available"):
            raise KeyError(task_id)
        return entry["value"]

    def has(self, task_id: str) -> bool:
        entry = self._data.get(task_id)
        return bool(entry and entry.get("available"))

    def task_ids(self) -> List[str]:
        return sorted(self._data)

    def _save(self) -> None:
        _atomic_write(
            self.path,
            json.dumps({"version": 1, "results": self._data}, indent=2, sort_keys=True),
        )
