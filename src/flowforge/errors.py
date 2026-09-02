"""Exception hierarchy for flowforge.

The split that matters is :class:`TransientError` vs :class:`PermanentError`.
Everything in the retry layer keys off it. A transport hiccup is transient and
worth another attempt; a malformed row is permanent and retrying it just burns
the deadline and writes the same error to the log five times.
"""

from __future__ import annotations

from typing import Optional, Sequence


class FlowForgeError(Exception):
    """Base class for every error raised by this package."""


class TransientError(FlowForgeError):
    """A failure that a later attempt could plausibly succeed at.

    Connection resets, HTTP 503, a lock held by another process, a file that is
    still being written. Retry policies retry this class by default.
    """


class PermanentError(FlowForgeError):
    """A failure that will fail identically on every attempt.

    Bad input, a missing column, a 404, an authentication rejection. Retrying
    is pure waste and it hides the real error behind a timeout.
    """


class CycleError(FlowForgeError):
    """The workflow graph contains a cycle.

    Carries the actual cycle as a node path so the message names the tasks
    involved rather than saying "graph is not a DAG".
    """

    def __init__(self, cycle: Sequence[str]) -> None:
        self.cycle = list(cycle)
        path = " -> ".join(self.cycle)
        super().__init__(f"workflow contains a cycle: {path}")


class UnknownTaskError(FlowForgeError):
    """A dependency names a task that is not in the workflow."""

    def __init__(self, task_id: str, referenced_by: Optional[str] = None) -> None:
        self.task_id = task_id
        self.referenced_by = referenced_by
        if referenced_by is None:
            super().__init__(f"unknown task {task_id!r}")
        else:
            super().__init__(
                f"task {referenced_by!r} depends on {task_id!r}, which is not defined"
            )


class DuplicateTaskError(FlowForgeError):
    """Two tasks share an id."""


class TaskTimeout(TransientError):
    """A task exceeded its per-task timeout.

    Transient by default: a timeout usually means "slow", not "impossible".
    Set ``retry_on`` on the policy if your workload disagrees.
    """


class DeadlineExceeded(FlowForgeError):
    """The whole run ran out of wall-clock budget."""


class Cancelled(FlowForgeError):
    """The task or run was cancelled before it produced a result."""


class RetryExhausted(FlowForgeError):
    """All retry attempts were used. Carries the last underlying error."""

    def __init__(self, attempts: int, last_error: BaseException) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"gave up after {attempts} attempt(s): "
            f"{type(last_error).__name__}: {last_error}"
        )


class CircuitOpen(TransientError):
    """The circuit breaker for this target is open, so the call was not made."""

    def __init__(self, name: str, retry_after: float) -> None:
        self.name = name
        self.retry_after = retry_after
        super().__init__(
            f"circuit {name!r} is open; not calling for another {retry_after:.1f}s"
        )


class ValidationError(FlowForgeError):
    """A workflow definition failed schema validation.

    Always names the offending key, and the source line when the loader knows it.
    """

    def __init__(
        self,
        message: str,
        *,
        key: Optional[str] = None,
        line: Optional[int] = None,
        source: Optional[str] = None,
    ) -> None:
        self.message = message
        self.key = key
        self.line = line
        self.source = source
        where = []
        if source:
            where.append(source)
        if line is not None:
            where.append(f"line {line}")
        if key:
            where.append(f"key {key!r}")
        prefix = " ".join(where)
        super().__init__(f"{prefix}: {message}" if prefix else message)


class ConnectorError(FlowForgeError):
    """A connector could not complete its step."""


class MissingDependency(FlowForgeError):
    """An optional third-party package is needed for this code path."""

    def __init__(self, package: str, purpose: str) -> None:
        self.package = package
        super().__init__(
            f"{purpose} needs the optional package {package!r}. "
            f"Install it with: pip install {package}"
        )
