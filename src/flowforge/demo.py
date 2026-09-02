"""A self-contained demo run: no network, no services, no configuration.

It builds a workflow shaped like the ones people actually ask for -- fetch two
sources, join them, write a file, mail a report, push some metrics -- and wires
in the three things that make production automation different from a script:

* the API endpoint fails twice before it works, so the retry policy is doing
  real work;
* the metrics push always fails, under ``on_failure=skip``, so you can see a
  failure that must *not* kill the run and a downstream task correctly marked
  ``skipped`` rather than pretended away;
* the email step carries a content-addressed idempotency key, so the second
  half of the demo can re-run the whole workflow and show the mail is not sent
  a second time.
"""

from __future__ import annotations

import os
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple

from .connectors.csv_excel import CsvConnector
from .connectors.email_smtp import SmtpConnector
from .connectors.http import HttpConnector
from .connectors.mock import FlakyTransport, MockHttpTransport
from .errors import TransientError
from .executor import ExecutionOptions, Executor
from .idempotency import IdempotencyGuard, JsonFileStore, content_key
from .observability import (
    RunLogger,
    explain_failure,
    metrics,
    render_gantt,
    summarise,
)
from .retry import ExponentialBackoff
from .state import RunState, StateStore
from .task import OnFailure, TaskContext
from .workflow import Workflow

__all__ = ["build_demo", "run_demo"]

CUSTOMERS = [
    {"id": "c-1", "name": "Northgate Freight", "region": "north"},
    {"id": "c-2", "name": "Halden Marine", "region": "south"},
    {"id": "c-3", "name": "Ravenna Tooling", "region": "south"},
]
ORDERS = [
    {"order_id": "o-100", "customer": "c-1", "amount": "1,204.50"},
    {"order_id": "o-101", "customer": "c-2", "amount": "318.00"},
    {"order_id": "o-102", "customer": "c-2", "amount": "(45.00)"},
    {"order_id": "o-103", "customer": "c-3", "amount": "2,900.10"},
]


def build_demo(
    workdir: str,
    *,
    outbox: Optional[List[Dict[str, Any]]] = None,
    api_failures: int = 2,
) -> Tuple[Workflow, Dict[str, Any]]:
    """Build the demo workflow and return it with the mock objects it uses."""
    flaky = FlakyTransport(api_failures, payload={"orders": ORDERS}, mode="status")
    steady = MockHttpTransport({"GET /customers": {"customers": CUSTOMERS}})
    orders_api = HttpConnector("https://api.internal", transport=flaky)
    customers_api = HttpConnector("https://api.internal", transport=steady)
    csv = CsvConnector(workdir)
    mailer = SmtpConnector(sender="reports@internal", dry_run=True, outbox=outbox)

    workflow = Workflow(
        "demo_pipeline",
        description="fetch, join, write, notify -- with the failure modes wired in",
    )

    workflow.add(
        orders_api.as_task(
            "fetch_orders",
            path="/orders",
            method="GET",
            retry=ExponentialBackoff(max_attempts=4, base=0.01, factor=2.0),
            timeout=5.0,
            description="poll the orders API (fails twice on purpose)",
        )
    )
    workflow.add(
        customers_api.as_task(
            "fetch_customers",
            path="/customers",
            method="GET",
            retry=ExponentialBackoff(max_attempts=3, base=0.01),
            timeout=5.0,
        )
    )

    @workflow.task("join", depends_on=["fetch_orders", "fetch_customers"])
    def join(ctx: TaskContext) -> List[Dict[str, Any]]:
        """Join orders to customers and coerce the amounts."""
        from .connectors.csv_excel import to_number

        orders = ctx.result("fetch_orders")["data"]["orders"]
        customers = {
            c["id"]: c for c in ctx.result("fetch_customers")["data"]["customers"]
        }
        time.sleep(0.004)
        rows = []
        for order in orders:
            customer = customers.get(order["customer"], {})
            rows.append(
                {
                    "order_id": order["order_id"],
                    "customer": customer.get("name", "unknown"),
                    "region": customer.get("region", "unknown"),
                    "amount": round(to_number(order["amount"]), 2),
                }
            )
        return rows

    @workflow.task("write_csv", depends_on=["join"])
    def write_csv(ctx: TaskContext) -> Dict[str, Any]:
        """Write the joined rows to a CSV, atomically."""
        rows = ctx.result("join", expect=list)
        return csv.write(
            "report.csv", rows, columns=["order_id", "customer", "region", "amount"]
        )

    def email_key(ctx: TaskContext) -> str:
        rows = ctx.result("join", expect=list)
        return content_key("email_report", date=ctx.param("date", "today"), rows=rows)

    @workflow.task(
        "email_report",
        depends_on=["join", "write_csv"],
        idempotency_key=email_key,
        description="send the report (exactly once per content)",
    )
    def email_report(ctx: TaskContext) -> Dict[str, Any]:
        rows = ctx.result("join", expect=list)
        total = sum(row["amount"] for row in rows)
        return mailer.run(
            ctx,
            to=["ops@internal"],
            subject=f"Daily order report ({len(rows)} orders)",
            body=f"{len(rows)} orders totalling {total:.2f}.",
        )

    @workflow.task(
        "push_metrics",
        depends_on=["join"],
        on_failure=OnFailure.SKIP,
        retry=ExponentialBackoff(max_attempts=2, base=0.01),
        description="best-effort metrics push; must not kill the report",
    )
    def push_metrics(ctx: TaskContext) -> None:
        raise TransientError("metrics gateway refused the connection")

    @workflow.task("update_dashboard", depends_on=["push_metrics"])
    def update_dashboard(ctx: TaskContext) -> str:
        return "dashboard updated"

    return workflow, {
        "flaky": flaky,
        "steady": steady,
        "mailer": mailer,
        "csv": csv,
        "workdir": workdir,
    }


def run_demo(
    workdir: Optional[str] = None, *, verbose: bool = False
) -> Tuple[RunState, RunState]:
    """Run the demo twice and print what happened. Returns both run states.

    The second run is the interesting one: same inputs, same content key, so
    the email is served from the idempotency store instead of being sent again.
    """
    temp: Optional[tempfile.TemporaryDirectory] = None
    if workdir is None:
        temp = tempfile.TemporaryDirectory(prefix="flowforge-demo-")
        workdir = temp.name
    try:
        outbox: List[Dict[str, Any]] = []
        workflow, parts = build_demo(workdir, outbox=outbox)
        store = StateStore(os.path.join(workdir, "runs"))
        guard = IdempotencyGuard(
            JsonFileStore(os.path.join(workdir, "idempotency.json"))
        )
        logger = RunLogger(enabled=verbose)
        executor = Executor(
            workflow,
            options=ExecutionOptions(max_workers=4, default_timeout=10.0),
            state_store=store,
            idempotency=guard,
            logger=logger,
        )

        print(_rule("workflow"))
        print(workflow.render())
        print()
        for warning in workflow.lint():
            print(f"lint: {warning}")

        print()
        print(_rule("run 1 of 2"))
        first = executor.run({"date": "2026-02-17"}, run_id="demo-1")
        _report(first, workflow)

        print()
        print(_rule("run 2 of 2 (same inputs, re-run after the partial failure)"))
        second = executor.run({"date": "2026-02-17"}, run_id="demo-2")
        _report(second, workflow)
        print()
        print(
            f"emails actually sent across both runs: {len(outbox)} "
            f"(the second run reused the idempotency key)"
        )
        print(f"api calls made: {parts['flaky'].calls} "
              f"(2 failed with 503 and were retried)")
        return first, second
    finally:
        if temp is not None:
            temp.cleanup()


def _report(state: RunState, workflow: Workflow) -> None:
    print(summarise(state))
    print()
    print(render_gantt(state))
    print()
    summary = metrics(state)
    print(
        f"success_rate={summary.success_rate:.2f}  retries={summary.retries}  "
        f"p50={summary.p50_duration:.3f}s  p95={summary.p95_duration:.3f}s  "
        f"slowest={summary.slowest_task}"
    )
    print()
    print(explain_failure(state, workflow.dag()))


def _rule(label: str) -> str:
    line = "=" * max(4, 72 - len(label) - 1)
    return f"{label} {line}"
