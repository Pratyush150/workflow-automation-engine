"""Connectors: the steps that touch the outside world.

Every connector answers the same two questions, because the engine needs both:

**What does it return?** A plain JSON-serialisable ``dict`` with ``ok``,
``data`` and ``meta``. Not a bespoke object -- a run's outputs get written to
the result archive so a resumed run can read them, and a type that will not
survive ``json.dumps`` silently breaks resume.

**Is it idempotent?** Declared, per connector, in ``idempotent``. Reading a
file twice is fine. Sending an email twice is not. The engine uses the flag:
:meth:`~flowforge.workflow.Workflow.lint` refuses to stay quiet about a
non-idempotent step with retries and no idempotency key, which is the exact
configuration that mails your customers twice.

Adding one is small: subclass :class:`Connector`, implement ``run``, set
``idempotent`` honestly.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Optional

from ..idempotency import content_key
from ..retry import NO_RETRY, RetryPolicy
from ..task import OnFailure, Task, TaskContext

__all__ = ["Connector", "connector_result"]


def connector_result(ok: bool = True, data: Any = None, **meta: Any) -> Dict[str, Any]:
    """Build the uniform connector return value."""
    return {"ok": bool(ok), "data": data, "meta": meta}


class Connector(ABC):
    """Base class for a workflow step that talks to something external."""

    #: Short name, used in idempotency keys and logs.
    name: str = "connector"
    #: Declared, not guessed: is running this twice with the same input safe?
    idempotent: bool = False

    @abstractmethod
    def run(self, ctx: TaskContext, **kwargs: Any) -> Dict[str, Any]:
        """Do the work and return :func:`connector_result`."""

    def key_material(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """The parts of a call that define "the same call again".

        Override when a call argument is noise -- a timestamp, a nonce -- and
        must not change the idempotency key.
        """
        return dict(kwargs)

    def as_task(
        self,
        task_id: str,
        *,
        depends_on: Iterable[str] = (),
        retry: RetryPolicy = NO_RETRY,
        timeout: Optional[float] = None,
        on_failure: Any = OnFailure.FAIL,
        tags: Iterable[str] = (),
        idempotency_key: Any = None,
        description: str = "",
        **kwargs: Any,
    ) -> Task:
        """Wrap a configured call as a :class:`~flowforge.task.Task`.

        ``idempotency_key="auto"`` derives a content-addressed key from the
        connector name and the call arguments, so the same call in a re-run is
        served from the store instead of being made again.
        """
        if idempotency_key == "auto":
            material = self.key_material(kwargs)

            def _auto_key(ctx: TaskContext, _material: Dict[str, Any] = material) -> str:
                return content_key(task_id, self.name, **_material)

            idempotency_key = _auto_key

        def _run(ctx: TaskContext) -> Dict[str, Any]:
            return self.run(ctx, **kwargs)

        _run.__name__ = f"{self.name}:{task_id}"
        return Task(
            id=task_id,
            fn=_run,
            depends_on=tuple(depends_on),
            retry=retry,
            timeout=timeout,
            on_failure=OnFailure(on_failure),
            tags=frozenset(tags) | {f"connector:{self.name}"},
            idempotency_key=idempotency_key,
            idempotent=self.idempotent,
            description=description or f"{self.name} step",
        )
