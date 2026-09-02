"""Mock connectors and transports. Used by the tests and the examples.

These are not toys: the flaky endpoint reproduces the exact shape of the
failure that retry logic exists for -- N failures, then success, with the call
count visible so a test can prove the retry happened and prove it stopped.
"""

from __future__ import annotations

import json as _json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..errors import TransientError
from ..task import TaskContext
from . import Connector, connector_result
from .http import HttpResponse

__all__ = [
    "FlakyTransport",
    "MockHttpTransport",
    "RecordingConnector",
    "ScriptedResponse",
]


@dataclass
class ScriptedResponse:
    """One canned HTTP response."""

    status: int = 200
    body: Any = ""
    headers: Dict[str, str] = field(default_factory=dict)

    def to_response(self, url: str) -> HttpResponse:
        body = self.body
        headers = dict(self.headers)
        if not isinstance(body, str):
            body = _json.dumps(body)
            headers.setdefault("Content-Type", "application/json")
        return HttpResponse(status=self.status, body=body, headers=headers, url=url)


class MockHttpTransport:
    """Serve scripted responses by ``"METHOD path"`` key, and record calls.

    A route may map to a single response or to a list, in which case each call
    consumes the next one and the last repeats. That is how "fails twice, then
    succeeds" is expressed.
    """

    def __init__(
        self,
        routes: Optional[Dict[str, Any]] = None,
        *,
        default: Optional[ScriptedResponse] = None,
    ) -> None:
        self.routes: Dict[str, List[ScriptedResponse]] = {}
        for key, value in (routes or {}).items():
            self.routes[key] = self._normalise(value)
        self.default = default
        self.calls: List[Dict[str, Any]] = []

    @staticmethod
    def _normalise(value: Any) -> List[ScriptedResponse]:
        if isinstance(value, ScriptedResponse):
            return [value]
        if isinstance(value, (list, tuple)):
            return [
                v if isinstance(v, ScriptedResponse) else ScriptedResponse(200, v)
                for v in value
            ]
        return [ScriptedResponse(200, value)]

    def route(self, key: str, *responses: Any) -> "MockHttpTransport":
        self.routes[key] = self._normalise(list(responses))
        return self

    def count(self, key: str = "") -> int:
        """Number of calls, optionally for one route key."""
        if not key:
            return len(self.calls)
        return sum(1 for call in self.calls if call["key"] == key)

    def __call__(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        timeout: float,
    ) -> HttpResponse:
        path = url.split("://", 1)[-1]
        path = "/" + path.split("/", 1)[1] if "/" in path else "/"
        key = f"{method} {path}"
        self.calls.append(
            {
                "key": key,
                "method": method,
                "url": url,
                "headers": dict(headers),
                "body": body.decode("utf-8") if body else "",
            }
        )
        scripted = self.routes.get(key) or self.routes.get(f"{method} {url}")
        if scripted is None:
            if self.default is not None:
                return self.default.to_response(url)
            return ScriptedResponse(404, {"error": f"no route for {key}"}).to_response(url)
        index = min(self.count(key) - 1, len(scripted) - 1)
        return scripted[index].to_response(url)


class FlakyTransport:
    """Fails the first ``fail_times`` calls, then succeeds. Counts everything.

    ``mode="status"`` returns a 503 (so the HTTP connector's classification is
    exercised); ``mode="raise"`` raises :class:`TransientError` (so the
    transport-failure path is exercised).
    """

    def __init__(
        self,
        fail_times: int = 2,
        *,
        payload: Any = None,
        mode: str = "status",
        retry_after: str = "",
    ) -> None:
        self.fail_times = fail_times
        self.payload = payload if payload is not None else {"ok": True}
        self.mode = mode
        self.retry_after = retry_after
        self.calls = 0

    def __call__(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        body: Optional[bytes],
        timeout: float,
    ) -> HttpResponse:
        self.calls += 1
        if self.calls <= self.fail_times:
            if self.mode == "raise":
                raise TransientError(f"connection reset (call {self.calls})")
            extra = {"Retry-After": self.retry_after} if self.retry_after else {}
            return ScriptedResponse(
                503, {"error": "service unavailable"}, extra
            ).to_response(url)
        return ScriptedResponse(200, self.payload).to_response(url)


class RecordingConnector(Connector):
    """A connector that records every call. Stands in for any side effect.

    ``fail_times`` makes it fail the first N calls with a transient error, which
    is how the idempotency tests prove a side effect happened exactly once
    across a failure and a re-run.
    """

    name = "mock"

    def __init__(
        self,
        *,
        idempotent: bool = False,
        fail_times: int = 0,
        value: Any = None,
    ) -> None:
        self.idempotent = idempotent
        self.fail_times = fail_times
        self.value = value
        self.calls: List[Dict[str, Any]] = []

    @property
    def call_count(self) -> int:
        return len(self.calls)

    def run(self, ctx: TaskContext, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append({"run_id": ctx.run_id, "task_id": ctx.task_id, **kwargs})
        if len(self.calls) <= self.fail_times:
            raise TransientError(f"mock failure {len(self.calls)}/{self.fail_times}")
        return connector_result(True, self.value if self.value is not None else kwargs)
