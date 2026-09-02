"""A workflow: a named set of tasks plus the graph they imply.

The graph is derived from each task's ``depends_on``. There is no separate edge
list to keep in sync, because two sources of truth for the same structure is
how you end up with a task that "is in the DAG" but never runs.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional

from .dag import Dag
from .errors import DuplicateTaskError, UnknownTaskError
from .retry import NO_RETRY, RetryPolicy
from .task import OnFailure, Step, Task

__all__ = ["Workflow"]


class Workflow:
    """Container for tasks, with validation and ordering delegated to :class:`Dag`."""

    def __init__(
        self,
        name: str,
        tasks: Iterable[Task] = (),
        *,
        description: str = "",
        deadline: Optional[float] = None,
    ) -> None:
        if not name:
            raise ValueError("workflow name must be a non-empty string")
        self.name = name
        self.description = description
        #: Wall-clock budget for the whole run, in seconds. ``None`` = unbounded.
        self.deadline = deadline
        self._tasks: Dict[str, Task] = {}
        for t in tasks:
            self.add(t)

    # -------------------------------------------------------------------- build

    def add(self, task: Task) -> Task:
        """Add a task. Raises on a duplicate id."""
        if not isinstance(task, Task):
            raise TypeError(f"expected Task, got {type(task).__name__}")
        if task.id in self._tasks:
            raise DuplicateTaskError(
                f"workflow {self.name!r} already has a task called {task.id!r}"
            )
        self._tasks[task.id] = task
        return task

    def add_step(self, step: Step, task_id: str, **kwargs: Any) -> Task:
        """Add a class-based :class:`~flowforge.task.Step`."""
        return self.add(step.as_task(task_id, **kwargs))

    def task(
        self,
        id: Optional[str] = None,
        *,
        depends_on: Iterable[str] = (),
        retry: RetryPolicy = NO_RETRY,
        timeout: Optional[float] = None,
        on_failure: Any = OnFailure.FAIL,
        tags: Iterable[str] = (),
        idempotency_key: Any = None,
        idempotent: bool = False,
        description: str = "",
    ) -> Callable[[Callable[..., Any]], Task]:
        """Decorator that builds a task and registers it in one step.

        >>> wf = Workflow("nightly")
        >>> @wf.task("extract")
        ... def extract(ctx):
        ...     return [1, 2, 3]
        >>> wf.ids
        ['extract']
        """

        def decorate(fn: Callable[..., Any]) -> Task:
            t = Task(
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
            return self.add(t)

        return decorate

    # -------------------------------------------------------------------- query

    def __contains__(self, task_id: object) -> bool:
        return task_id in self._tasks

    def __len__(self) -> int:
        return len(self._tasks)

    def __getitem__(self, task_id: str) -> Task:
        try:
            return self._tasks[task_id]
        except KeyError:
            raise UnknownTaskError(task_id) from None

    def __iter__(self):
        return iter(self._tasks.values())

    @property
    def tasks(self) -> List[Task]:
        return list(self._tasks.values())

    @property
    def ids(self) -> List[str]:
        return list(self._tasks)

    def dag(self) -> Dag:
        """Build the dependency graph. Cheap; rebuilt on every call by design."""
        graph = Dag()
        for task in self._tasks.values():
            graph.add_node(task.id, task.depends_on)
        return graph

    def validate(self) -> None:
        """Raise on unknown dependencies or cycles."""
        self.dag().validate()

    def lint(self) -> List[str]:
        """Non-fatal warnings a human should look at before shipping the workflow."""
        warnings: List[str] = []
        graph = self.dag()
        for node in graph.isolated_nodes():
            if len(self._tasks) > 1:
                warnings.append(
                    f"task {node!r} has no dependencies and nothing depends on it; "
                    f"it will run, but it is not connected to the rest of the workflow"
                )
        for task in self._tasks.values():
            if task.retry.max_attempts > 1 and not task.idempotent and not task.idempotency_key:
                warnings.append(
                    f"task {task.id!r} retries up to {task.retry.max_attempts} times but "
                    f"is not declared idempotent and has no idempotency key; a retry "
                    f"after a partial success will repeat its side effect"
                )
            if task.timeout is None and task.retry.max_attempts > 1:
                warnings.append(
                    f"task {task.id!r} retries but has no timeout; one hung attempt "
                    f"blocks the run for as long as the call hangs"
                )
        return warnings

    def order(self) -> List[str]:
        return self.dag().topological_order()

    def levels(self) -> List[List[str]]:
        return self.dag().levels()

    def render(self) -> str:
        """ASCII graph with each task's policy annotated."""
        marks = {}
        for task in self._tasks.values():
            bits = []
            if task.retry.max_attempts > 1:
                bits.append(f"retry x{task.retry.max_attempts}")
            if task.timeout:
                bits.append(f"timeout {task.timeout:g}s")
            if task.idempotency_key is not None:
                bits.append("idempotent-key")
            if task.on_failure is not OnFailure.FAIL:
                bits.append(f"on_failure={task.on_failure.value}")
            marks[task.id] = f"[{', '.join(bits)}]" if bits else ""
        return self.dag().render(marks)
