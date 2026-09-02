"""Idempotency keys and a result cache.

The scenario this exists for: a nightly workflow of six tasks. Task four sends
280 invoice emails. Task five writes to a database and fails. Somebody re-runs
the workflow.

Without an idempotency layer, 280 people get a second invoice, and you find out
from them. Retries inside a task have the same problem in miniature: a POST
that timed out may well have been received.

So every side-effecting task can declare a **content-addressed key** derived
from what it is about to do -- not from the wall clock, not from a random id.
Same inputs, same key. Before running, the engine asks the store whether that
key already completed; if so it reuses the recorded result and does not run the
task at all.

Three record states, and the difference between the last two is the whole point:

``completed`` -- the effect definitely happened. Reuse the result.
``failed``    -- the task raised, and *our process survived to write that down*.
                 The effect probably did not happen. Safe to run again.
``started``   -- we wrote "about to do it" and never wrote anything else. The
                 process was killed mid-flight. We genuinely do not know
                 whether the email went out. That is not a bug in this library,
                 it is the at-least-once/at-most-once boundary, and the only
                 honest thing to do is let the operator choose the policy:
                 ``rerun`` (risk a duplicate), ``skip`` (risk a gap), or
                 ``error`` (stop and make a human look).
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, Iterator, List, Optional, Protocol, Tuple

from .clock import SYSTEM_CLOCK, Clock
from .errors import FlowForgeError

__all__ = [
    "AmbiguousReplay",
    "BeginResult",
    "IdempotencyGuard",
    "IdempotencyStore",
    "JsonFileStore",
    "MemoryStore",
    "Record",
    "canonical_json",
    "content_key",
    "digest",
]

STARTED = "started"
COMPLETED = "completed"
FAILED = "failed"


class AmbiguousReplay(FlowForgeError):
    """A key was left in ``started`` state by a run that never finished."""


def canonical_json(value: Any) -> str:
    """Stable JSON for hashing: sorted keys, no whitespace jitter.

    Anything not JSON-native falls back to ``repr`` inside a marker object, so
    key generation never explodes on an unexpected type -- but the caller gets
    a stable key only if that ``repr`` is stable.
    """

    def default(obj: Any) -> Any:
        if isinstance(obj, (set, frozenset)):
            return {"__set__": sorted(map(repr, obj))}
        if isinstance(obj, bytes):
            return {"__bytes_sha256__": hashlib.sha256(obj).hexdigest()}
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return {"__repr__": repr(obj)}

    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=default, ensure_ascii=False
    )


def digest(value: Any, length: int = 16) -> str:
    """Short stable hash of any value. Used for input/output digests in state."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()[:length]


def content_key(namespace: str, *parts: Any, **fields: Any) -> str:
    """Build a content-addressed idempotency key.

    ``namespace`` is normally the task id, so two tasks with identical payloads
    do not collide. The digest covers positional parts and keyword fields.

    >>> content_key("send_invoice", customer="c-1", amount=120)
    'send_invoice:379fb57310fbc4e9'
    """
    payload = {"parts": list(parts), "fields": fields}
    return f"{namespace}:{digest(payload)}"


@dataclass
class Record:
    """One idempotency entry."""

    key: str
    status: str
    run_id: str = ""
    task_id: str = ""
    created_at: str = ""
    updated_at: str = ""
    value: Any = None
    #: False when the value could not be persisted (file store, non-JSON value).
    value_available: bool = False
    value_digest: str = ""
    error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Record":
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in data.items() if k in known})


class IdempotencyStore(Protocol):
    """Pluggable persistence for idempotency records."""

    def get(self, key: str) -> Optional[Record]: ...

    def put(self, record: Record) -> None: ...

    def delete(self, key: str) -> None: ...

    def keys(self) -> List[str]: ...


class MemoryStore:
    """In-process store. Correct for a single run, useless across restarts."""

    def __init__(self) -> None:
        self._data: Dict[str, Record] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Record]:
        with self._lock:
            return self._data.get(key)

    def put(self, record: Record) -> None:
        with self._lock:
            self._data[record.key] = record

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def keys(self) -> List[str]:
        with self._lock:
            return sorted(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator[Record]:
        return iter(list(self._data.values()))


class JsonFileStore:
    """Single-JSON-file store, written atomically.

    Writes go to a temp file in the same directory and are then ``os.replace``d
    over the target, so a crash mid-write leaves the previous file intact rather
    than a half-written one. That matters: a truncated idempotency store is
    worse than no store, because it silently re-sends.

    Suitable for one host. For several machines running the same workflow, put a
    real database behind the :class:`IdempotencyStore` protocol -- the engine
    only needs those four methods.
    """

    def __init__(self, path: str) -> None:
        self.path = str(path)
        self._lock = threading.Lock()
        self._data: Dict[str, Record] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (json.JSONDecodeError, OSError):
            # A corrupt store must not be silently treated as empty: that means
            # re-sending everything. Move it aside and start clean, loudly.
            os.replace(self.path, self.path + ".corrupt")
            return
        for key, item in raw.get("records", {}).items():
            self._data[key] = Record.from_dict(item)

    def _flush(self) -> None:
        parent = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(parent, exist_ok=True)
        payload = {
            "version": 1,
            "records": {k: v.to_dict() for k, v in self._data.items()},
        }
        fd, tmp = tempfile.mkstemp(dir=parent, prefix=".ff-idem-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True, default=str)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def get(self, key: str) -> Optional[Record]:
        with self._lock:
            return self._data.get(key)

    def put(self, record: Record) -> None:
        record = _make_persistable(record)
        with self._lock:
            self._data[record.key] = record
            self._flush()

    def delete(self, key: str) -> None:
        with self._lock:
            if self._data.pop(key, None) is not None:
                self._flush()

    def keys(self) -> List[str]:
        with self._lock:
            return sorted(self._data)


def _make_persistable(record: Record) -> Record:
    """Blank out a value that cannot survive a JSON round trip."""
    if not record.value_available:
        return record
    try:
        json.dumps(record.value)
    except (TypeError, ValueError):
        record.value = None
        record.value_available = False
    return record


@dataclass
class BeginResult:
    """What :meth:`IdempotencyGuard.begin` found."""

    state: str  # "fresh" | "cached" | "ambiguous"
    record: Optional[Record] = None

    @property
    def cached(self) -> bool:
        return self.state == "cached"


class IdempotencyGuard:
    """Wraps a store with the begin/complete/fail protocol.

    ``on_ambiguous`` decides what happens when a key was left ``started``:

    ``"rerun"``  at-least-once. The default. Correct for anything you can make
                 naturally idempotent (upserts, ``PUT``, writing a file).
    ``"skip"``   at-most-once. Correct when a duplicate is worse than a gap and
                 a human will reconcile.
    ``"error"``  refuse to guess. Correct for payments.
    """

    def __init__(
        self,
        store: Optional[IdempotencyStore] = None,
        *,
        clock: Clock = SYSTEM_CLOCK,
        on_ambiguous: str = "rerun",
        log: Optional[Callable[..., None]] = None,
    ) -> None:
        if on_ambiguous not in ("rerun", "skip", "error"):
            raise ValueError(f"unknown on_ambiguous policy {on_ambiguous!r}")
        self.store: IdempotencyStore = store if store is not None else MemoryStore()
        self.clock = clock
        self.on_ambiguous = on_ambiguous
        self._log = log or (lambda *a, **k: None)

    def lookup(self, key: str) -> Optional[Record]:
        return self.store.get(key)

    def begin(self, key: str, *, run_id: str = "", task_id: str = "") -> BeginResult:
        """Claim ``key``. Returns whether the work should actually happen."""
        existing = self.store.get(key)
        now = self.clock.now().isoformat()
        if existing is not None:
            if existing.status == COMPLETED:
                return BeginResult("cached", existing)
            if existing.status == STARTED:
                if self.on_ambiguous == "error":
                    raise AmbiguousReplay(
                        f"idempotency key {key!r} was left in-flight by run "
                        f"{existing.run_id!r}. The side effect may or may not have "
                        f"happened. Resolve it by hand, or set "
                        f"on_ambiguous='rerun'/'skip'."
                    )
                if self.on_ambiguous == "skip":
                    self._log(
                        "idempotency.ambiguous_skipped", key=key, prior_run=existing.run_id
                    )
                    return BeginResult("cached", existing)
                self._log(
                    "idempotency.ambiguous_rerun", key=key, prior_run=existing.run_id
                )
        record = Record(
            key=key,
            status=STARTED,
            run_id=run_id,
            task_id=task_id,
            created_at=(existing.created_at if existing else now),
            updated_at=now,
        )
        self.store.put(record)
        return BeginResult("fresh", record)

    def complete(self, key: str, value: Any, *, run_id: str = "", task_id: str = "") -> Record:
        """Mark ``key`` done and remember the result."""
        existing = self.store.get(key)
        now = self.clock.now().isoformat()
        record = Record(
            key=key,
            status=COMPLETED,
            run_id=run_id or (existing.run_id if existing else ""),
            task_id=task_id or (existing.task_id if existing else ""),
            created_at=(existing.created_at if existing else now),
            updated_at=now,
            value=value,
            value_available=True,
            value_digest=digest(value),
        )
        self.store.put(record)
        return record

    def fail(self, key: str, error: BaseException, *, run_id: str = "") -> Record:
        """Mark ``key`` failed. The process survived, so a re-run is safe."""
        existing = self.store.get(key)
        now = self.clock.now().isoformat()
        record = Record(
            key=key,
            status=FAILED,
            run_id=run_id or (existing.run_id if existing else ""),
            task_id=(existing.task_id if existing else ""),
            created_at=(existing.created_at if existing else now),
            updated_at=now,
            error=f"{type(error).__name__}: {error}",
        )
        self.store.put(record)
        return record

    def call(
        self,
        key: str,
        fn: Callable[[], Any],
        *,
        run_id: str = "",
        task_id: str = "",
    ) -> Tuple[Any, bool]:
        """Run ``fn`` at most once per key. Returns ``(value, was_cached)``."""
        outcome = self.begin(key, run_id=run_id, task_id=task_id)
        if outcome.cached:
            rec = outcome.record
            assert rec is not None
            if not rec.value_available:
                self._log("idempotency.cached_without_value", key=key)
            return (rec.value if rec.value_available else None), True
        try:
            value = fn()
        except BaseException as exc:  # noqa: BLE001 - recorded and re-raised
            self.fail(key, exc, run_id=run_id)
            raise
        self.complete(key, value, run_id=run_id, task_id=task_id)
        return value, False
