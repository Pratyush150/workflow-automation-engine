"""Tasks: a callable plus the metadata the engine needs to run it safely.

Two things here are deliberate.

**No globals.** A task receives a :class:`TaskContext` and returns a value.
Upstream outputs are read through ``ctx.result("task_id")``. Nothing is passed
by mutating module state, because the moment two tasks run in parallel that
becomes a race you will debug at 2am.

**Failure policy is part of the definition, not the call site.** Whether a
failing task should stop the run, be tolerated, or be treated as "carry on
without me" is a property of what the task *does*. A metrics push that fails
should not kill an invoicing run; the invoicing step failing should.
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
)

from .errors import Cancelled, PermanentError
from .retry import NO_RETRY, RetryPolicy

__all__ = [
    "CancelToken",
    "MissingResult",
    "OnFailure",
    "Step",
    "Task",
    "TaskContext",
    "task",
]

T = TypeVar("T")
_MISSING = object()


class OnFailure(str, Enum):
    """What a task failing means for the rest of the run.

    ``FAIL``     -- the default. Downstream tasks are marked ``upstream_failed``
                    and the run is a failure. Independent branches still run.
    ``SKIP``     -- the task is a failure but not a fatal one. Downstream tasks
                    are marked ``skipped``. The run finishes ``degraded``.
    ``CONTINUE`` -- downstream tasks still run, and see ``None`` as this task's
                    result. Only correct when downstream genuinely tolerates a
                    missing input. The run finishes ``degraded``.
    """

    FAIL = "fail"
    SKIP = "skip"
    CONTINUE = "continue"


class CancelToken:
    """Cooperative cancellation.

    Python cannot forcibly kill a thread, so a task that wants to be
    interruptible must check ``ctx.cancel.raise_if_cancelled()`` inside its
    loops. The engine sets the token on timeout and on fail-fast so
    well-behaved tasks stop instead of finishing work nobody will read.
    """

    __slots__ = ("_set", "_reason")

    def __init__(self) -> None:
        self._set = False
        self._reason = ""

    def cancel(self, reason: str = "cancelled") -> None:
        self._set = True
        self._reason = reason

    @property
    def cancelled(self) -> bool:
        return self._set

    @property
    def reason(self) -> str:
        return self._reason

    def raise_if_cancelled(self) -> None:
        """Raise :class:`Cancelled` if the token has been set."""
        if self._set:
            raise Cancelled(self._reason)


@dataclass
class TaskContext:
    """Everything a task is allowed to see.

    Passed explicitly to every task. Read upstream outputs with
    :meth:`result`, run parameters with :meth:`param`.
    """

    run_id: str
    task_id: str
    attempt: int = 1
    params: Mapping[str, Any] = field(default_factory=dict)
    results: Mapping[str, Any] = field(default_factory=dict)
    deadline: Optional[float] = None
    cancel: CancelToken = field(default_factory=CancelToken)
    log: Callable[..., None] = lambda *a, **k: None
    scratch: Dict[str, Any] = field(default_factory=dict)

    def result(self, task_id: str, expect: Optional[Type[T]] = None) -> Any:
        """Output of an upstream task, optionally type-checked.

        Raises :class:`PermanentError` rather than ``KeyError`` when the task is
        not there: a missing upstream result means the workflow is wired wrong,
        and that will not fix itself on a retry.
        """
        if task_id not in self.results:
            available = ", ".join(sorted(self.results)) or "<none>"
            raise PermanentError(
                f"task {self.task_id!r} asked for the result of {task_id!r}, "
                f"which has not produced one. Available: {available}. "
                f"Did you forget to add it to depends_on?"
            )
        value = self.results[task_id]
        if isinstance(value, MissingResult):
            raise PermanentError(
                f"task {self.task_id!r} needs the output of {task_id!r}, but "
                f"{value.reason}"
            )
        if expect is not None and not isinstance(value, expect):
            raise PermanentError(
                f"task {self.task_id!r} expected {task_id!r} to return "
                f"{expect.__name__}, got {type(value).__name__}"
            )
        return value

    def param(
        self,
        name: str,
        default: Any = _MISSING,
        expect: Optional[Type[T]] = None,
    ) -> Any:
        """Run parameter lookup with an actionable error when it is absent."""
        if name not in self.params:
            if default is _MISSING:
                known = ", ".join(sorted(self.params)) or "<none>"
                raise PermanentError(
                    f"missing run parameter {name!r} (task {self.task_id!r}). "
                    f"Parameters supplied: {known}"
                )
            return default
        value = self.params[name]
        if expect is not None and not isinstance(value, expect):
            raise PermanentError(
                f"parameter {name!r} should be {expect.__name__}, "
                f"got {type(value).__name__}"
            )
        return value

    def time_left(self, now: float) -> Optional[float]:
        """Seconds until the run deadline, or ``None`` if there is no deadline."""
        if self.deadline is None:
            return None
        return self.deadline - now


@dataclass(frozen=True)
class MissingResult:
    """Placeholder for an upstream output that could not be restored.

    A resumed run inherits the *fact* that a task succeeded, but the value it
    returned only survives if it could be written to the result archive as
    JSON. Rather than handing a downstream task ``None`` and letting it write
    an empty report, the engine puts one of these in the context and
    :meth:`TaskContext.result` raises with the reason.
    """

    task_id: str
    reason: str


class Step(ABC):
    """Base class for a task with state or configuration.

    Use it when a step needs constructor arguments, a connection object, or
    setup and teardown. ``Step`` instances are turned into :class:`Task` by
    :meth:`as_task`, so the engine treats them identically to plain functions.
    """

    #: Re-running this step with the same inputs produces no extra side effect.
    idempotent: bool = False

    @abstractmethod
    def run(self, ctx: TaskContext) -> Any:
        """Do the work. Return the value downstream tasks will read."""

    def idempotency_key(self, ctx: TaskContext) -> Optional[str]:
        """Override to give the step a content-addressed key. ``None`` disables."""
        return None

    def as_task(self, task_id: str, **kwargs: Any) -> "Task":
        """Wrap this step as a :class:`Task`."""
        kwargs.setdefault("idempotent", self.idempotent)
        if "idempotency_key" not in kwargs:
            probe = type(self).idempotency_key is not Step.idempotency_key
            if probe:
                kwargs["idempotency_key"] = self.idempotency_key
        return Task(id=task_id, fn=self.run, **kwargs)


@dataclass(frozen=True)
class Task:
    """A unit of work plus the policy that governs it."""

    id: str
    fn: Callable[..., Any]
    depends_on: Sequence[str] = ()
    retry: RetryPolicy = NO_RETRY
    timeout: Optional[float] = None
    on_failure: OnFailure = OnFailure.FAIL
    tags: FrozenSet[str] = frozenset()
    #: ``str`` for a fixed key, or a callable receiving the context. ``None``
    #: means "do not consult the idempotency store for this task".
    idempotency_key: Union[str, Callable[[TaskContext], Optional[str]], None] = None
    #: Declared, not inferred: does re-running this produce a second side effect?
    idempotent: bool = False
    description: str = ""

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("task id must be a non-empty string")
        if not callable(self.fn):
            raise TypeError(f"task {self.id!r}: fn must be callable")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError(f"task {self.id!r}: timeout must be > 0")
        object.__setattr__(self, "depends_on", tuple(self.depends_on))
        object.__setattr__(self, "tags", frozenset(self.tags))
        object.__setattr__(self, "on_failure", OnFailure(self.on_failure))

    def __call__(self, ctx: TaskContext) -> Any:
        """Run the wrapped callable.

        Plain zero-argument functions are supported: a step that needs nothing
        from the context should not be forced to accept one.
        """
        if _accepts_context(self.fn):
            return self.fn(ctx)
        return self.fn()

    def key_for(self, ctx: TaskContext) -> Optional[str]:
        """Resolve this task's idempotency key for a given context."""
        spec = self.idempotency_key
        if spec is None:
            return None
        if callable(spec):
            return spec(ctx)
        return str(spec)

    def with_(self, **changes: Any) -> "Task":
        """Copy with fields replaced. Tasks are frozen on purpose."""
        return replace(self, **changes)


def _accepts_context(fn: Callable[..., Any]) -> bool:
    """True if ``fn`` takes at least one positional argument."""
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):  # builtins, C callables
        return True
    for param in sig.parameters.values():
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            return True
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            return True
    return False


def task(
    id: Optional[str] = None,
    *,
    depends_on: Iterable[str] = (),
    retry: RetryPolicy = NO_RETRY,
    timeout: Optional[float] = None,
    on_failure: Union[OnFailure, str] = OnFailure.FAIL,
    tags: Iterable[str] = (),
    idempotency_key: Union[str, Callable[[TaskContext], Optional[str]], None] = None,
    idempotent: bool = False,
    description: str = "",
) -> Callable[[Callable[..., Any]], Task]:
    """Decorator turning a function into a :class:`Task`.

    >>> @task("extract", retry=NO_RETRY)
    ... def extract(ctx):
    ...     return [1, 2, 3]
    >>> extract.id
    'extract'
    """

    def decorate(fn: Callable[..., Any]) -> Task:
        return Task(
            id=id or fn.__name__,
            fn=fn,
            depends_on=tuple(depends_on),
            retry=retry,
            timeout=timeout,
            on_failure=OnFailure(on_failure),
            tags=frozenset(tags),
            idempotency_key=idempotency_key,
            idempotent=idempotent,
            description=description or (fn.__doc__ or "").strip().split("\n")[0],
        )

    return decorate
