"""Connectors: uniform shape, honest idempotency flags, real behaviour."""

from __future__ import annotations

import os
import sys

import pytest

from flowforge import Executor, TaskContext, Workflow
from flowforge.connectors import Connector, connector_result
from flowforge.connectors.csv_excel import CsvConnector, aggregate, to_number
from flowforge.connectors.email_smtp import SmtpConnector, render_message
from flowforge.connectors.filesystem import FilesystemConnector
from flowforge.connectors.http import HttpConnector, HttpResponse
from flowforge.connectors.mock import (
    FlakyTransport,
    MockHttpTransport,
    RecordingConnector,
    ScriptedResponse,
)
from flowforge.connectors.shell import ShellConnector
from flowforge.connectors.webhook import WebhookConnector, sign_payload
from flowforge.errors import ConnectorError, PermanentError, TaskTimeout, TransientError
from flowforge.retry import ExponentialBackoff
from flowforge.task import CancelToken


def context(task_id: str = "t") -> TaskContext:
    return TaskContext(run_id="r", task_id=task_id, cancel=CancelToken())


# ------------------------------------------------------------------------ http


def test_http_success_parses_json_and_reports_meta():
    connector = HttpConnector(
        "https://api.test", transport=MockHttpTransport({"GET /things": {"n": 1}})
    )
    result = connector.run(context(), path="/things")
    assert result["ok"] is True
    assert result["data"] == {"n": 1}
    assert result["meta"]["status"] == 200
    assert result["meta"]["url"] == "https://api.test/things"


def test_http_5xx_is_transient_and_4xx_is_permanent():
    transport = MockHttpTransport(
        {
            "GET /down": ScriptedResponse(503, {"error": "unavailable"}, {"Retry-After": "5"}),
            "GET /missing": ScriptedResponse(404, {"error": "nope"}),
        }
    )
    connector = HttpConnector("https://api.test", transport=transport)
    with pytest.raises(TransientError) as transient:
        connector.run(context(), path="/down")
    assert "Retry-After: 5" in str(transient.value)
    with pytest.raises(PermanentError):
        connector.run(context(), path="/missing")


def test_http_retries_a_flaky_endpoint_and_then_succeeds():
    transport = FlakyTransport(2, payload={"orders": []})
    workflow = Workflow("http_retry")
    workflow.add(
        HttpConnector("https://api.test", transport=transport).as_task(
            "poll",
            path="/orders",
            retry=ExponentialBackoff(max_attempts=4, base=0.001),
        )
    )
    state = Executor(workflow).run()
    assert state.status.value == "succeeded"
    assert state.tasks["poll"].attempts == 3
    assert transport.calls == 3


def test_http_idempotency_flag_follows_the_method():
    connector = HttpConnector(transport=MockHttpTransport())
    assert connector.as_task("get", path="/x", method="GET").idempotent is True
    assert connector.as_task("put", path="/x", method="PUT").idempotent is True
    assert connector.as_task("post", path="/x", method="POST").idempotent is False


def test_http_response_helpers():
    response = HttpResponse(200, '{"a": 1}', {"Content-Type": "application/json"}, "u")
    assert response.json() == {"a": 1}
    assert response.header("content-type") == "application/json"
    with pytest.raises(PermanentError):
        HttpResponse(200, "not json", {}, "u").json()


# --------------------------------------------------------------------- webhook


def test_webhook_signs_the_payload_and_sends_a_stable_delivery_id():
    transport = MockHttpTransport({"POST /hook": {"received": True}})
    connector = WebhookConnector("https://hooks.test/hook", secret="s3cret", transport=transport)
    first = connector.run(context(), payload={"total": 3}, event="report.ready")
    second = connector.run(context(), payload={"total": 3}, event="report.ready")

    assert first["meta"]["delivery_id"] == second["meta"]["delivery_id"]
    sent = transport.calls[0]
    signature = sent["headers"]["X-Flowforge-Signature"]
    timestamp = sent["headers"]["X-Flowforge-Timestamp"]
    assert signature == sign_payload("s3cret", timestamp, sent["body"])
    assert connector.idempotent is False


# ----------------------------------------------------------------------- shell


def test_shell_captures_output_and_exit_code():
    connector = ShellConnector()
    result = connector.run(context(), command=[sys.executable, "-c", "print('hello')"])
    assert result["ok"] is True
    assert result["data"]["stdout"].strip() == "hello"
    assert result["meta"]["returncode"] == 0


def test_shell_raises_with_the_stderr_tail_on_failure():
    connector = ShellConnector()
    with pytest.raises(ConnectorError) as excinfo:
        connector.run(
            context(),
            command=[sys.executable, "-c", "import sys;sys.stderr.write('bad thing\\n');sys.exit(3)"],
        )
    assert "exited 3" in str(excinfo.value)
    assert "bad thing" in str(excinfo.value)


def test_shell_timeout_kills_the_child():
    connector = ShellConnector(timeout=0.05)
    with pytest.raises(TaskTimeout):
        connector.run(context(), command=[sys.executable, "-c", "import time;time.sleep(5)"])


def test_shell_can_treat_specific_exit_codes_as_retryable():
    connector = ShellConnector()
    with pytest.raises(TransientError):
        connector.run(
            context(),
            command=[sys.executable, "-c", "raise SystemExit(75)"],
            retry_exit_codes=[75],
        )


def test_shell_rejects_an_empty_command():
    with pytest.raises(ConnectorError):
        ShellConnector().run(context(), command=[])


# ------------------------------------------------------------------ filesystem


def test_watch_ignores_files_that_are_still_being_written(tmp_path):
    drop = tmp_path / "drop"
    drop.mkdir()
    (drop / "done.csv").write_text("a\n", encoding="utf-8")
    (drop / "uploading.csv").write_text("b\n", encoding="utf-8")
    old = 1000.0
    os.utime(drop / "done.csv", (old, old))

    connector = FilesystemConnector(str(tmp_path))
    result = connector.watch("drop", suffix=".csv", stable_for=30.0, now=old + 60)
    names = [entry["name"] for entry in result["data"]]
    assert names == ["done.csv"]
    assert result["meta"]["unstable"] == ["uploading.csv"]


def test_archive_is_safe_to_repeat(tmp_path):
    drop = tmp_path / "drop"
    drop.mkdir()
    (drop / "orders.csv").write_text("x\n", encoding="utf-8")
    connector = FilesystemConnector(str(tmp_path))

    first = connector.archive("drop/orders.csv", "archive")
    assert first["meta"]["already_archived"] is False
    second = connector.archive("drop/orders.csv", "archive")
    assert second["meta"]["already_archived"] is True
    assert os.path.exists(str(tmp_path / "archive" / "orders.csv"))
    assert connector.idempotent is True


def test_write_is_atomic_and_leaves_no_partial_file(tmp_path):
    connector = FilesystemConnector(str(tmp_path))
    connector.write("out/report.txt", "hello")
    assert (tmp_path / "out" / "report.txt").read_text(encoding="utf-8") == "hello"
    assert not [p for p in os.listdir(tmp_path / "out") if p.endswith(".partial")]


def test_paths_cannot_escape_the_connector_root(tmp_path):
    connector = FilesystemConnector(str(tmp_path / "root"))
    with pytest.raises(ConnectorError):
        connector.path("../outside.txt")


# ------------------------------------------------------------------------- csv


def test_csv_round_trip_with_a_bom_and_required_columns(tmp_path):
    path = tmp_path / "in.csv"
    path.write_text("﻿id,amount\n1,10.00\n2,(5.00)\n", encoding="utf-8")
    connector = CsvConnector(str(tmp_path))
    result = connector.read("in.csv", require_columns=["id", "amount"])
    assert result["meta"]["rows"] == 2
    assert result["data"][0]["id"] == "1", "the BOM must not leak into the header"

    written = connector.write("out.csv", result["data"], columns=["id", "amount"])
    assert written["meta"]["rows"] == 2
    assert (tmp_path / "out.csv").read_text(encoding="utf-8").startswith("id,amount")


def test_csv_missing_column_is_a_permanent_error(tmp_path):
    (tmp_path / "in.csv").write_text("id\n1\n", encoding="utf-8")
    with pytest.raises(PermanentError) as excinfo:
        CsvConnector(str(tmp_path)).read("in.csv", require_columns=["id", "amount"])
    assert "amount" in str(excinfo.value)


def test_to_number_handles_spreadsheet_reality():
    assert to_number("1,234.50") == 1234.5
    assert to_number("(45.00)") == -45.0
    assert to_number("$12") == 12.0
    assert to_number("", default=0.0) == 0.0
    with pytest.raises(PermanentError):
        to_number("N/A")


def test_aggregate_groups_and_sums():
    rows = [
        {"region": "north", "net": "10"},
        {"region": "south", "net": "5"},
        {"region": "north", "net": "2.50"},
    ]
    assert aggregate(rows, ["region"], ["net"]) == [
        {"region": "north", "count": 2, "net": 12.5},
        {"region": "south", "count": 1, "net": 5.0},
    ]


def test_csv_transform_selects_filters_and_sorts():
    connector = CsvConnector()
    rows = [
        {"id": "2", "amount": "5", "junk": "x"},
        {"id": "1", "amount": "50", "junk": "y"},
        {"id": "3", "amount": "1", "junk": "z"},
    ]
    result = connector.transform(
        rows,
        select=["id", "amount"],
        numeric=["amount"],
        where=lambda row: row["amount"] > 2,
        sort_by=["id"],
    )
    assert [row["id"] for row in result["data"]] == ["1", "2"]
    assert result["meta"]["dropped"] == 1


# ----------------------------------------------------------------------- email


def test_email_dry_run_renders_without_sending():
    connector = SmtpConnector(sender="a@b", dry_run=True)
    result = connector.run(context(), to=["c@d"], subject="Hi", body="Body")
    assert result["meta"]["dry_run"] is True
    assert "Subject: Hi" in result["data"]["rendered"]
    assert len(connector.outbox) == 1
    assert connector.idempotent is False, "sending email is never idempotent"


def test_email_needs_a_recipient():
    with pytest.raises(PermanentError):
        SmtpConnector(dry_run=True).run(context(), to=[], subject="x", body="y")


def test_message_key_is_content_addressed():
    a = SmtpConnector.message_key("notify", ["a@b"], "Subject", "Body")
    b = SmtpConnector.message_key("notify", ["a@b"], "Subject", "Body")
    c = SmtpConnector.message_key("notify", ["a@b"], "Subject", "Different")
    assert a == b != c
    assert render_message("a@b", ["c@d"], "S", "B")["To"] == "c@d"


# --------------------------------------------------------------------- generic


def test_every_connector_declares_its_idempotency():
    connectors = [
        HttpConnector(transport=MockHttpTransport()),
        WebhookConnector("https://x/y"),
        ShellConnector(),
        FilesystemConnector("."),
        CsvConnector("."),
        SmtpConnector(),
        RecordingConnector(),
    ]
    for connector in connectors:
        assert isinstance(connector, Connector)
        assert isinstance(connector.idempotent, bool)
        assert connector.name


def test_connector_results_share_one_shape():
    result = connector_result(True, [1, 2], count=2)
    assert set(result) == {"ok", "data", "meta"}
    assert result["meta"]["count"] == 2


def test_auto_idempotency_key_is_derived_from_the_call():
    connector = RecordingConnector(value={"sent": 1})
    task_a = connector.as_task("send", idempotency_key="auto", to="a@b")
    task_b = connector.as_task("send", idempotency_key="auto", to="a@b")
    task_c = connector.as_task("send", idempotency_key="auto", to="c@d")
    ctx = context("send")
    assert task_a.key_for(ctx) == task_b.key_for(ctx)
    assert task_a.key_for(ctx) != task_c.key_for(ctx)


def test_connector_tasks_carry_a_connector_tag():
    task = RecordingConnector().as_task("step")
    assert "connector:mock" in task.tags
