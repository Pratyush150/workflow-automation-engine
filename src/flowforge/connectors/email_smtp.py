"""SMTP email, with the send guarded and a dry-run that renders the message.

This is the connector the whole idempotency story exists for. Email is the
canonical non-idempotent side effect: there is no ``PUT /email/123``, no undo,
and the recipient is a person who will notice.

Rules encoded here:

* ``idempotent = False``. Always. Nothing in this class can make a second send
  invisible.
* ``dry_run=True`` by default. A workflow you are developing does not mail
  anybody by accident; it returns the rendered message so you can assert on it.
* :meth:`SmtpConnector.message_key` gives a content-addressed idempotency key
  built from recipients, subject and body. Wire it into the task and a re-run
  after a partial failure will not send twice.

``smtplib`` is stdlib, so importing this module never fails. Connecting needs a
reachable server, which the test suite does not have and does not want.
"""

from __future__ import annotations

import smtplib
import ssl
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Sequence

from ..errors import PermanentError, TransientError
from ..idempotency import content_key
from ..task import TaskContext
from . import Connector, connector_result

__all__ = ["SmtpConnector", "render_message"]


def render_message(
    sender: str,
    to: Sequence[str],
    subject: str,
    body: str,
    *,
    cc: Sequence[str] = (),
    html: str = "",
) -> EmailMessage:
    """Build an :class:`email.message.EmailMessage`."""
    message = EmailMessage()
    message["From"] = sender
    message["To"] = ", ".join(to)
    if cc:
        message["Cc"] = ", ".join(cc)
    message["Subject"] = subject
    message.set_content(body)
    if html:
        message.add_alternative(html, subtype="html")
    return message


class SmtpConnector(Connector):
    """Send an email over SMTP, or render it and stop."""

    name = "email"
    #: Never true. Sending twice sends twice.
    idempotent = False

    def __init__(
        self,
        host: str = "localhost",
        port: int = 25,
        *,
        sender: str = "flowforge@localhost",
        username: str = "",
        password: str = "",
        use_tls: bool = False,
        timeout: float = 15.0,
        dry_run: bool = True,
        outbox: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.sender = sender
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.timeout = timeout
        self.dry_run = dry_run
        #: Every send (real or dry) is appended here, which is what tests assert on.
        self.outbox: List[Dict[str, Any]] = outbox if outbox is not None else []

    @staticmethod
    def message_key(
        task_id: str, to: Sequence[str], subject: str, body: str
    ) -> str:
        """Content-addressed key for one specific message."""
        return content_key(task_id, "email", to=sorted(to), subject=subject, body=body)

    def run(
        self,
        ctx: TaskContext,
        *,
        to: Sequence[str] = (),
        subject: str = "",
        body: str = "",
        cc: Sequence[str] = (),
        html: str = "",
        sender: str = "",
    ) -> Dict[str, Any]:
        recipients = [r for r in to if r]
        if not recipients:
            raise PermanentError("email step called with no recipients")
        message = render_message(
            sender or self.sender, recipients, subject, body, cc=cc, html=html
        )
        entry = {
            "to": recipients,
            "cc": list(cc),
            "subject": subject,
            "body": body,
            "run_id": ctx.run_id,
            "task_id": ctx.task_id,
            "dry_run": self.dry_run,
        }
        if self.dry_run:
            self.outbox.append(entry)
            return connector_result(
                True,
                {"rendered": message.as_string()},
                dry_run=True,
                recipients=len(recipients),
            )
        ctx.cancel.raise_if_cancelled()
        try:
            with smtplib.SMTP(self.host, self.port, timeout=self.timeout) as smtp:
                if self.use_tls:
                    smtp.starttls(context=ssl.create_default_context())
                if self.username:
                    smtp.login(self.username, self.password)
                smtp.send_message(message)
        except (smtplib.SMTPAuthenticationError, smtplib.SMTPRecipientsRefused) as exc:
            # Bad credentials or a bad address will fail identically forever.
            raise PermanentError(f"smtp rejected the message: {exc}") from exc
        except (smtplib.SMTPException, OSError) as exc:
            raise TransientError(f"smtp send failed: {exc}") from exc
        self.outbox.append(entry)
        return connector_result(
            True, {"sent": True}, dry_run=False, recipients=len(recipients)
        )

    def key_material(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "to": sorted(kwargs.get("to", ())),
            "subject": kwargs.get("subject", ""),
            "body": kwargs.get("body", ""),
        }
