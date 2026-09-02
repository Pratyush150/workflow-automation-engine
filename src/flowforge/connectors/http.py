"""HTTP connector with a pluggable transport.

The transport is injected. That is not an abstraction for its own sake: it is
what lets the retry behaviour, the status-code classification and the
``Retry-After`` handling be tested exactly, offline, in milliseconds. The
default transport is ``urllib.request`` from the standard library, so nothing
needs installing.

Status classification, which is where naive HTTP steps go wrong:

* ``2xx`` -- success.
* ``429`` and ``5xx`` (except 501) -- :class:`TransientError`, so a retry
  policy will try again, honouring ``Retry-After`` when the server sends it.
* every other ``4xx`` -- :class:`PermanentError`. Your request is wrong.
  Sending it again unchanged is not a strategy.
"""

from __future__ import annotations

import json as _json
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Sequence

from ..errors import PermanentError, TransientError
from ..retry import retryable_status
from ..task import TaskContext
from . import Connector, connector_result

__all__ = ["HttpConnector", "HttpResponse", "urllib_transport"]

#: Methods that are safe to repeat, by definition in RFC 9110.
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})


@dataclass
class HttpResponse:
    """A response, independent of which transport produced it."""

    status: int
    body: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    url: str = ""

    def json(self) -> Any:
        """Parse the body as JSON, with a permanent error if it is not JSON."""
        try:
            return _json.loads(self.body)
        except ValueError as exc:
            raise PermanentError(
                f"{self.url} returned status {self.status} with a body that is "
                f"not JSON: {exc}"
            ) from None

    def header(self, name: str, default: str = "") -> str:
        lowered = {k.lower(): v for k, v in self.headers.items()}
        return lowered.get(name.lower(), default)


Transport = Callable[[str, str, Dict[str, str], Optional[bytes], float], HttpResponse]


def urllib_transport(
    method: str,
    url: str,
    headers: Dict[str, str],
    body: Optional[bytes],
    timeout: float,
) -> HttpResponse:
    """Default transport: stdlib ``urllib``. Never used by the test suite."""
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return HttpResponse(
                status=response.status,
                body=response.read().decode("utf-8", "replace"),
                headers={k: v for k, v in response.headers.items()},
                url=url,
            )
    except urllib.error.HTTPError as exc:  # a response, not a transport failure
        return HttpResponse(
            status=exc.code,
            body=exc.read().decode("utf-8", "replace"),
            headers={k: v for k, v in (exc.headers or {}).items()},
            url=url,
        )
    except urllib.error.URLError as exc:
        raise TransientError(f"{method} {url}: {exc.reason}") from exc


class HttpConnector(Connector):
    """Make an HTTP request and classify the result."""

    name = "http"
    #: Depends on the method; :meth:`as_task` narrows it per call.
    idempotent = False

    def __init__(
        self,
        base_url: str = "",
        *,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 10.0,
        transport: Optional[Transport] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = dict(headers or {})
        self.timeout = timeout
        self.transport: Transport = transport or urllib_transport

    def url_for(self, path: str) -> str:
        if path.startswith(("http://", "https://")) or not self.base_url:
            return path
        return f"{self.base_url}/{path.lstrip('/')}"

    def run(
        self,
        ctx: TaskContext,
        *,
        path: str = "",
        method: str = "GET",
        json: Any = None,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        expect: Sequence[int] = (),
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Perform the request. Raises on a non-success status."""
        method = method.upper()
        url = self.url_for(path)
        merged = {**self.headers, **(headers or {})}
        body = data
        if json is not None:
            body = _json.dumps(json).encode("utf-8")
            merged.setdefault("Content-Type", "application/json")
        ctx.cancel.raise_if_cancelled()
        response = self.transport(
            method, url, merged, body, timeout or self.timeout
        )
        ok_statuses = tuple(expect) if expect else tuple(range(200, 300))
        if response.status not in ok_statuses:
            self._raise_for_status(method, url, response)
        parsed: Any = response.body
        if response.header("content-type").startswith("application/json") or (
            response.body[:1] in ("{", "[")
        ):
            try:
                parsed = response.json()
            except PermanentError:
                parsed = response.body
        return connector_result(
            True,
            parsed,
            status=response.status,
            url=url,
            method=method,
            bytes=len(response.body),
        )

    def _raise_for_status(self, method: str, url: str, response: HttpResponse) -> None:
        snippet = response.body[:200].replace("\n", " ")
        message = f"{method} {url} -> {response.status}: {snippet}"
        if retryable_status(response.status):
            retry_after = response.header("retry-after")
            if retry_after:
                message += f" (Retry-After: {retry_after})"
            raise TransientError(message)
        raise PermanentError(message)

    def key_material(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        material = dict(kwargs)
        material.pop("headers", None)  # auth tokens rotate; the call is the same
        material["url"] = self.url_for(str(material.pop("path", "")))
        return material

    def as_task(self, task_id: str, **kwargs: Any) -> Any:
        """Set ``idempotent`` from the HTTP method before building the task."""
        method = str(kwargs.get("method", "GET")).upper()
        original = type(self).idempotent
        self.idempotent = method in IDEMPOTENT_METHODS
        try:
            return super().as_task(task_id, **kwargs)
        finally:
            self.idempotent = original
