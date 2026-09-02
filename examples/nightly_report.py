#!/usr/bin/env python3
"""Nightly report pipeline: extract, transform, write a CSV, notify.

The shape of nine out of ten "can you automate this" requests. What makes it
more than a script:

* the two extracts run in parallel because they do not depend on each other;
* the transform validates its input and raises a *permanent* error on bad data,
  so a malformed row fails fast instead of being retried five times;
* the notification carries a content-addressed idempotency key, so re-running
  the workflow after a downstream failure does not send the report twice;
* the run is persisted, so ``flowforge history`` can answer "did last night's
  report go out?" tomorrow morning.

Run it:  python3 examples/nightly_report.py
"""

from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, List

import _bootstrap  # noqa: F401  (path setup)

from flowforge import (
    ExecutionOptions,
    Executor,
    ExponentialBackoff,
    IdempotencyGuard,
    JsonFileStore,
    StateStore,
    TaskContext,
    Workflow,
    content_key,
    metrics,
    render_gantt,
    summarise,
)
from flowforge.connectors.csv_excel import CsvConnector, aggregate, to_number
from flowforge.connectors.email_smtp import SmtpConnector
from flowforge.connectors.http import HttpConnector
from flowforge.connectors.mock import MockHttpTransport
from flowforge.errors import PermanentError

SALES = [
    {"invoice": "INV-1001", "region": "north", "net": "1,204.50", "vat": "240.90"},
    {"invoice": "INV-1002", "region": "south", "net": "318.00", "vat": "63.60"},
    {"invoice": "INV-1003", "region": "south", "net": "2,900.10", "vat": "580.02"},
    {"invoice": "INV-1004", "region": "north", "net": "(45.00)", "vat": "(9.00)"},
]
RATES = {"north": 0.19, "south": 0.21}


def build(workdir: str, outbox: List[Dict[str, Any]]) -> Workflow:
    """Build the workflow. Separated so the tests can drive it too."""
    api = HttpConnector(
        "https://erp.internal",
        transport=MockHttpTransport({"GET /invoices": {"invoices": SALES}}),
    )
    csv = CsvConnector(workdir)
    mailer = SmtpConnector(sender="reports@internal", dry_run=True, outbox=outbox)
    workflow = Workflow("nightly_report", description="Daily sales report")

    workflow.add(
        api.as_task(
            "extract_invoices",
            path="/invoices",
            method="GET",
            retry=ExponentialBackoff(max_attempts=3, base=0.01),
            timeout=10.0,
            description="pull yesterday's invoices",
        )
    )

    @workflow.task("extract_rates")
    def extract_rates(ctx: TaskContext) -> Dict[str, float]:
        """Regional commission rates. Independent of the invoice extract."""
        return dict(RATES)

    @workflow.task("transform", depends_on=["extract_invoices", "extract_rates"])
    def transform(ctx: TaskContext) -> List[Dict[str, Any]]:
        """Coerce amounts, compute commission, and reject bad rows loudly."""
        invoices = ctx.result("extract_invoices")["data"]["invoices"]
        rates = ctx.result("extract_rates", expect=dict)
        rows: List[Dict[str, Any]] = []
        for invoice in invoices:
            region = invoice["region"]
            if region not in rates:
                # Permanent: a region with no rate will still have no rate on
                # attempt five. Fail now, with the offending value in the message.
                raise PermanentError(
                    f"invoice {invoice['invoice']} has region {region!r}, "
                    f"which has no commission rate (known: {sorted(rates)})"
                )
            net = to_number(invoice["net"])
            rows.append(
                {
                    "invoice": invoice["invoice"],
                    "region": region,
                    "net": round(net, 2),
                    "vat": round(to_number(invoice["vat"]), 2),
                    "commission": round(net * rates[region], 2),
                }
            )
        return rows

    @workflow.task("summarise_by_region", depends_on=["transform"])
    def summarise_by_region(ctx: TaskContext) -> List[Dict[str, Any]]:
        rows = ctx.result("transform", expect=list)
        return aggregate(rows, ["region"], ["net", "vat", "commission"])

    @workflow.task("write_csv", depends_on=["transform"])
    def write_csv(ctx: TaskContext) -> Dict[str, Any]:
        rows = ctx.result("transform", expect=list)
        return csv.write(
            "nightly_report.csv",
            rows,
            columns=["invoice", "region", "net", "vat", "commission"],
        )

    def notify_key(ctx: TaskContext) -> str:
        return content_key(
            "notify",
            date=ctx.param("date", "unknown"),
            totals=ctx.result("summarise_by_region"),
        )

    @workflow.task(
        "notify",
        depends_on=["summarise_by_region", "write_csv"],
        idempotency_key=notify_key,
        description="email the summary exactly once per (date, content)",
    )
    def notify(ctx: TaskContext) -> Dict[str, Any]:
        totals = ctx.result("summarise_by_region", expect=list)
        path = ctx.result("write_csv")["data"]
        lines = [
            f"{row['region']}: {row['count']} invoices, "
            f"net {row['net']:.2f}, commission {row['commission']:.2f}"
            for row in totals
        ]
        return mailer.run(
            ctx,
            to=["finance@internal"],
            subject=f"Nightly report {ctx.param('date', '')}".strip(),
            body="\n".join(lines) + f"\n\nCSV: {path}\n",
        )

    return workflow


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="flowforge-nightly-") as workdir:
        outbox: List[Dict[str, Any]] = []
        workflow = build(workdir, outbox)
        executor = Executor(
            workflow,
            options=ExecutionOptions(max_workers=3),
            state_store=StateStore(os.path.join(workdir, "runs")),
            idempotency=IdempotencyGuard(
                JsonFileStore(os.path.join(workdir, "idempotency.json"))
            ),
        )
        print(workflow.render())
        print()
        state = executor.run({"date": "2026-02-17"})
        print(summarise(state))
        print()
        print(render_gantt(state))
        print()
        with open(os.path.join(workdir, "nightly_report.csv"), encoding="utf-8") as fh:
            print(fh.read().rstrip())
        print()
        print(f"emails sent: {len(outbox)} -> {outbox[0]['subject']!r}")

        second = executor.run({"date": "2026-02-17"})
        print(
            f"second run: {second.status.value}, "
            f"notify was {second.tasks['notify'].status.value}, "
            f"emails still {len(outbox)}"
        )
        print(f"p95 task duration: {metrics(state).p95_duration:.3f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
