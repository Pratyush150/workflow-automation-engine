#!/usr/bin/env python3
"""Poll two APIs, reconcile them, and survive a flaky endpoint.

The endpoint here fails twice with HTTP 503 before it works. That is not a
contrived case: it is what a rate-limited or restarting service looks like from
the outside, and it is the reason retry policies exist.

What the example demonstrates:

* exponential backoff with **deterministic jitter** (an injected ``Random``),
  so the delay sequence is reproducible instead of "roughly a second-ish";
* a **circuit breaker** shared by both API tasks via a ``circuit:billing`` tag:
  once the dependency is clearly down, further calls fail immediately instead
  of adding load to something that is already struggling;
* 4xx is *not* retried. The second half of the run asks for a customer that
  does not exist, gets a 404, and fails once -- fast, with the real message --
  under ``on_failure=continue`` so the reconciliation still produces a report;
* the reconciliation itself reports differences instead of raising, because
  "the two systems disagree" is data, not an exception.

Run it:  python3 examples/api_poll_reconcile.py
"""

from __future__ import annotations

import json
import os
import random
import tempfile
from typing import Any, Dict

import _bootstrap  # noqa: F401

from flowforge import (
    CircuitBreaker,
    ResultArchive,
    StateStore,
    ExecutionOptions,
    Executor,
    JitteredExponentialBackoff,
    OnFailure,
    TaskContext,
    Workflow,
    explain_failure,
    render_gantt,
    summarise,
)
from flowforge.connectors.http import HttpConnector
from flowforge.connectors.mock import FlakyTransport, MockHttpTransport, ScriptedResponse

LEDGER = [
    {"invoice": "INV-1001", "amount": 1204.50},
    {"invoice": "INV-1002", "amount": 318.00},
    {"invoice": "INV-1003", "amount": 2900.10},
]
BANK = [
    {"reference": "INV-1001", "settled": 1204.50},
    {"reference": "INV-1002", "settled": 318.00},
    {"reference": "INV-1009", "settled": 75.00},
]


def build() -> Workflow:
    ledger_api = HttpConnector(
        "https://ledger.internal",
        transport=FlakyTransport(2, payload={"invoices": LEDGER}, mode="status",
                                 retry_after="1"),
    )
    bank_api = HttpConnector(
        "https://bank.internal",
        transport=MockHttpTransport(
            {
                "GET /settlements": {"settlements": BANK},
                "GET /customers/c-404": ScriptedResponse(404, {"error": "no such customer"}),
            }
        ),
    )
    policy = JitteredExponentialBackoff(
        max_attempts=4, base=0.02, factor=2.0, jitter=0.5, rng=random.Random(11)
    )

    workflow = Workflow("api_poll_reconcile", description="Ledger vs bank reconciliation")
    workflow.add(
        ledger_api.as_task(
            "poll_ledger",
            path="/invoices",
            method="GET",
            retry=policy,
            timeout=5.0,
            tags=["circuit:billing"],
            description="flaky on purpose: two 503s then a 200",
        )
    )
    workflow.add(
        bank_api.as_task(
            "poll_bank",
            path="/settlements",
            method="GET",
            retry=policy,
            timeout=5.0,
            tags=["circuit:billing"],
        )
    )
    workflow.add(
        bank_api.as_task(
            "enrich_customer",
            path="/customers/c-404",
            method="GET",
            retry=policy,
            timeout=5.0,
            on_failure=OnFailure.CONTINUE,
            description="404 is permanent: one attempt, then carry on without it",
        )
    )

    @workflow.task("reconcile", depends_on=["poll_ledger", "poll_bank", "enrich_customer"])
    def reconcile(ctx: TaskContext) -> Dict[str, Any]:
        """Compare the two sides. Differences are output, not exceptions."""
        invoices = {
            row["invoice"]: row["amount"]
            for row in ctx.result("poll_ledger")["data"]["invoices"]
        }
        settlements = {
            row["reference"]: row["settled"]
            for row in ctx.result("poll_bank")["data"]["settlements"]
        }
        matched, missing, unexpected, mismatched = [], [], [], []
        for reference, amount in sorted(invoices.items()):
            if reference not in settlements:
                missing.append(reference)
            elif abs(settlements[reference] - amount) > 0.005:
                mismatched.append(reference)
            else:
                matched.append(reference)
        for reference in sorted(settlements):
            if reference not in invoices:
                unexpected.append(reference)
        return {
            "matched": matched,
            "missing_from_bank": missing,
            "unexpected_in_bank": unexpected,
            "amount_mismatch": mismatched,
            "enrichment": ctx.result("enrich_customer") is not None,
        }

    return workflow


def main() -> int:
    workflow = build()
    breaker = CircuitBreaker("billing", failure_threshold=6, reset_timeout=5.0)
    workdir = tempfile.mkdtemp(prefix="flowforge-reconcile-")
    store = StateStore(os.path.join(workdir, "runs"))
    executor = Executor(
        workflow,
        options=ExecutionOptions(max_workers=3),
        breakers={"billing": breaker},
        state_store=store,
    )
    state = executor.run()
    print(summarise(state))
    print()
    print(render_gantt(state))
    print()
    ledger = state.tasks["poll_ledger"]
    print(f"poll_ledger attempts: {ledger.attempts}  decisions: {ledger.retry_reasons}")
    enrich = state.tasks["enrich_customer"]
    print(f"enrich_customer attempts: {enrich.attempts} ({enrich.error_type}: {enrich.error})")
    print(f"circuit 'billing' after the run: {breaker.state.value}, failures={breaker.failures}")
    print()
    print(explain_failure(state, workflow.dag()))
    print()
    archive = ResultArchive(store.archive_path(state))
    print("reconciliation result (read back from the run's result archive):")
    print(json.dumps(archive.get("reconcile"), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
