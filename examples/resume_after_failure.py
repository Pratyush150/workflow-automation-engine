#!/usr/bin/env python3
"""A run that dies halfway, and what happens next.

This is the scenario that separates a workflow engine from a shell script.

Run 1 emails three customers, then the ledger API is down and the run fails.
The obvious fix -- "just run it again" -- emails all three customers a second
time. That is the bug this whole library is arranged around.

What actually happens here:

* **resume** reads the last run's durable state and executes only the tasks
  that did not already succeed. ``email_customers`` is inherited, not re-run.
* the **idempotency key** on ``email_customers`` is a second, independent
  guard: even a full re-run from scratch finds the key completed and serves the
  recorded result instead of sending. Belt and braces, because the two protect
  against different mistakes -- resume protects against re-running the
  workflow, the key protects against re-running the *task*.
* the outbox count is printed after every run, so you can see it stay at 3.

Run it:  python3 examples/resume_after_failure.py
"""

from __future__ import annotations

import os
import tempfile
from typing import Any, Dict, List

import _bootstrap  # noqa: F401

from flowforge import (
    ExecutionOptions,
    Executor,
    ExponentialBackoff,
    IdempotencyGuard,
    JsonFileStore,
    StateStore,
    TaskContext,
    TaskStatus,
    Workflow,
    content_key,
    explain_failure,
    summarise,
)
from flowforge.connectors.email_smtp import SmtpConnector
from flowforge.errors import TransientError

INVOICES = [
    {"invoice": "INV-2001", "customer": "ops@northgate.example", "amount": 1204.50},
    {"invoice": "INV-2002", "customer": "ap@halden.example", "amount": 318.00},
    {"invoice": "INV-2003", "customer": "finance@ravenna.example", "amount": 2900.10},
]


def build(outbox: List[Dict[str, Any]], ledger_calls: List[str]) -> Workflow:
    mailer = SmtpConnector(sender="billing@internal", dry_run=True, outbox=outbox)
    workflow = Workflow("invoice_run", description="Send invoices, then post the ledger")

    @workflow.task("fetch_invoices")
    def fetch_invoices(ctx: TaskContext) -> List[Dict[str, Any]]:
        return [dict(row) for row in INVOICES]

    @workflow.task("build_batch", depends_on=["fetch_invoices"])
    def build_batch(ctx: TaskContext) -> Dict[str, Any]:
        rows = ctx.result("fetch_invoices", expect=list)
        return {
            "batch_id": ctx.param("batch_id"),
            "count": len(rows),
            "total": round(sum(row["amount"] for row in rows), 2),
            "invoices": rows,
        }

    def email_key(ctx: TaskContext) -> str:
        batch = ctx.result("build_batch", expect=dict)
        return content_key("email_customers", batch_id=batch["batch_id"], invoices=batch["invoices"])

    @workflow.task(
        "email_customers",
        depends_on=["build_batch"],
        idempotency_key=email_key,
        description="the side effect that must not happen twice",
    )
    def email_customers(ctx: TaskContext) -> Dict[str, Any]:
        batch = ctx.result("build_batch", expect=dict)
        for invoice in batch["invoices"]:
            mailer.run(
                ctx,
                to=[invoice["customer"]],
                subject=f"Invoice {invoice['invoice']}",
                body=f"Amount due: {invoice['amount']:.2f}",
            )
        return {"sent": batch["count"], "batch_id": batch["batch_id"]}

    @workflow.task(
        "post_ledger",
        depends_on=["email_customers"],
        retry=ExponentialBackoff(max_attempts=2, base=0.01),
        description="down during run 1",
    )
    def post_ledger(ctx: TaskContext) -> str:
        ledger_calls.append(ctx.run_id)
        if not ctx.param("ledger_up", False):
            raise TransientError("ledger API: connection refused")
        return "posted"

    @workflow.task("mark_complete", depends_on=["post_ledger", "build_batch"])
    def mark_complete(ctx: TaskContext) -> str:
        return f"batch {ctx.result('build_batch', expect=dict)['batch_id']} closed"

    return workflow


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="flowforge-resume-") as workdir:
        outbox: List[Dict[str, Any]] = []
        ledger_calls: List[str] = []
        workflow = build(outbox, ledger_calls)
        store = StateStore(os.path.join(workdir, "runs"))
        guard = IdempotencyGuard(JsonFileStore(os.path.join(workdir, "idempotency.json")))
        executor = Executor(
            workflow,
            options=ExecutionOptions(max_workers=2),
            state_store=store,
            idempotency=guard,
        )
        params = {"batch_id": "2026-02-17-A"}

        print("=== run 1: the ledger is down ===")
        first = executor.run({**params, "ledger_up": False}, run_id="run-1")
        print(summarise(first))
        print(explain_failure(first, workflow.dag()))
        print(f"emails sent so far: {len(outbox)}")

        print()
        print("=== resume: only what did not succeed ===")
        previous = store.load("run-1", workflow.name)
        print(f"already done: {previous.completed_tasks()}")
        print(f"to run:       {previous.resume_from(workflow.ids)}")
        second = executor.run(
            {**params, "ledger_up": True}, run_id="run-2", resume=previous
        )
        print(summarise(second))
        print(
            "email_customers status on the resumed run: "
            f"{second.tasks['email_customers'].status.value}"
        )
        print(f"emails sent so far: {len(outbox)}   ledger attempts: {len(ledger_calls)}")

        print()
        print("=== a full re-run from scratch, no resume ===")
        third = executor.run({**params, "ledger_up": True}, run_id="run-3")
        print(summarise(third))
        print(
            "email_customers status: "
            f"{third.tasks['email_customers'].status.value} "
            "(served from the idempotency store)"
        )
        print(f"emails sent in total: {len(outbox)} -- still three, one per customer")
        assert len(outbox) == 3, outbox
        assert second.tasks["email_customers"].status is TaskStatus.SUCCEEDED
        assert third.tasks["email_customers"].status is TaskStatus.CACHED
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
