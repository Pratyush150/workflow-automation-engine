"""Outbound webhook with a signature and a delivery id.

Two details that separate a webhook step from "POST some JSON":

**Signing.** The receiver has to be able to tell your call from anybody else's.
HMAC-SHA256 over ``timestamp.body`` with a shared secret, sent as
``X-Flowforge-Signature``. The timestamp is inside the signed material so an
old, captured request cannot be replayed later.

**A stable delivery id.** ``X-Idempotency-Key`` is derived from the payload,
not from a random UUID. If we retry, the receiver sees the same key and can
drop the duplicate. A random id per attempt hands the receiver two events and
makes deduplication their problem -- and they will not solve it.
"""

from __future__ import annotations

import hashlib
import hmac
import json as _json
from typing import Any, Dict, Optional

from ..idempotency import content_key, digest
from ..task import TaskContext
from . import Connector, connector_result
from .http import HttpConnector, Transport

__all__ = ["WebhookConnector", "sign_payload"]


def sign_payload(secret: str, timestamp: str, body: str) -> str:
    """``hex(HMAC_SHA256(secret, "<timestamp>.<body>"))``."""
    material = f"{timestamp}.{body}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), material, hashlib.sha256).hexdigest()


class WebhookConnector(Connector):
    """POST a JSON payload to a URL, signed and de-duplicable."""

    name = "webhook"
    #: A POST is not idempotent. The delivery id lets the *receiver* make it so,
    #: which is not the same thing and is not our claim to make.
    idempotent = False

    def __init__(
        self,
        url: str,
        *,
        secret: str = "",
        timeout: float = 10.0,
        transport: Optional[Transport] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.url = url
        self.secret = secret
        self.http = HttpConnector(timeout=timeout, transport=transport, headers=headers)

    def run(
        self,
        ctx: TaskContext,
        *,
        payload: Any = None,
        event: str = "workflow.event",
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        body = {
            "event": event,
            "run_id": ctx.run_id,
            "task_id": ctx.task_id,
            "payload": payload,
        }
        encoded = _json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
        delivery_id = content_key("webhook", event, digest(body))
        sent_headers = {
            "Content-Type": "application/json",
            "X-Idempotency-Key": delivery_id,
            "X-Flowforge-Run": ctx.run_id,
        }
        if self.secret:
            # Derived from the payload, so a retry re-sends an identical,
            # still-valid signature rather than a fresh one.
            timestamp = digest(encoded, 8)
            sent_headers["X-Flowforge-Timestamp"] = timestamp
            sent_headers["X-Flowforge-Signature"] = sign_payload(
                self.secret, timestamp, encoded
            )
        sent_headers.update(headers or {})
        result = self.http.run(
            ctx,
            path=self.url,
            method="POST",
            data=encoded.encode("utf-8"),
            headers=sent_headers,
        )
        result["meta"]["delivery_id"] = delivery_id
        return connector_result(True, result["data"], **result["meta"])

    def key_material(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return {"url": self.url, "event": kwargs.get("event"), "payload": kwargs.get("payload")}
