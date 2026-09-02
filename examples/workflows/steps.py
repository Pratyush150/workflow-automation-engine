"""Task functions referenced by ``nightly_rollup.yaml``.

Keeping the code in Python and the wiring in YAML is usually the right split:
the file that operations edits (schedule, retries, timeouts, which steps run)
is separate from the file that engineering edits (what a step actually does).

The CLI puts the workflow file's directory on ``sys.path``, so ``python:steps:extract``
resolves relative to the YAML file.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List

from flowforge.connectors.csv_excel import aggregate, to_number
from flowforge.errors import PermanentError
from flowforge.task import TaskContext

ORDERS = [
    {"order_id": "A-1", "region": "north", "amount": "1,204.50"},
    {"order_id": "A-2", "region": "south", "amount": "318.00"},
    {"order_id": "A-3", "region": "south", "amount": "2,900.10"},
    {"order_id": "A-4", "region": "north", "amount": "(45.00)"},
]


def extract(ctx: TaskContext) -> List[Dict[str, Any]]:
    """Stand-in for a source system. Returns raw, string-typed rows."""
    ctx.log("extract.rows", count=len(ORDERS))
    return [dict(row) for row in ORDERS]


def validate_rows(ctx: TaskContext, *, required: List[str] = ()) -> List[Dict[str, Any]]:
    """Reject rows missing a required column. Bad input is permanent."""
    rows = ctx.result("extract", expect=list)
    required = list(required) or ["order_id", "region", "amount"]
    for index, row in enumerate(rows, start=1):
        missing = [column for column in required if column not in row]
        if missing:
            raise PermanentError(f"row {index} is missing column(s) {missing}")
    return rows


def rollup(ctx: TaskContext) -> List[Dict[str, Any]]:
    """Group by region and total the amounts."""
    rows = ctx.result("validate", expect=list)
    numeric = [
        {**row, "amount": to_number(row["amount"])}
        for row in rows
    ]
    return aggregate(numeric, ["region"], ["amount"])


def write_csv(ctx: TaskContext, *, filename: str = "rollup.csv") -> str:
    """Write the rollup to ``<out_dir>/<filename>``, atomically."""
    import csv as _csv

    out_dir = ctx.param("out_dir", "out")
    os.makedirs(out_dir, exist_ok=True)
    rows = ctx.result("rollup", expect=list)
    path = os.path.join(out_dir, filename)
    tmp = path + ".partial"
    with open(tmp, "w", encoding="utf-8", newline="") as handle:
        writer = _csv.DictWriter(handle, fieldnames=["region", "count", "amount"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    os.replace(tmp, path)
    ctx.log("write_csv.done", path=path, rows=len(rows))
    return path


def notify(ctx: TaskContext, *, to: str = "ops@internal") -> Dict[str, Any]:
    """Dry-run notification. Returns what would have been sent."""
    rows = ctx.result("rollup", expect=list)
    path = ctx.result("write_csv", expect=str)
    body = "\n".join(
        f"{row['region']}: {row['count']} orders, {row['amount']:.2f}" for row in rows
    )
    return {"to": to, "subject": "Nightly rollup", "body": body, "attachment": path}
