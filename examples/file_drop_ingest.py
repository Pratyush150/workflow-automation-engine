#!/usr/bin/env python3
"""File-drop ingest: watch a directory, load what is finished, archive it.

The failure this is built around: a partner drops a 40 MB CSV over SFTP, your
watcher sees it 200 ms in, and you ingest a truncated file. Nothing errors.
The numbers are just wrong.

So ``watch`` only reports files whose mtime is at least ``stable_for`` seconds
old, and the example plants a file that is still "being written" to prove the
watcher leaves it alone. Archiving is idempotent: re-running after a crash
finds the file already in ``archive/`` and reports ``already_archived`` instead
of failing.

Run it:  python3 examples/file_drop_ingest.py
"""

from __future__ import annotations

import os
import tempfile
import time
from typing import Any, Dict, List

import _bootstrap  # noqa: F401

from flowforge import (
    ExecutionOptions,
    Executor,
    OnFailure,
    StateStore,
    TaskContext,
    Workflow,
    render_gantt,
    summarise,
)
from flowforge.connectors.csv_excel import CsvConnector, to_number
from flowforge.connectors.filesystem import FilesystemConnector

def seed_drop_directory(drop: str) -> None:
    """Two finished files and one that is still uploading."""
    os.makedirs(drop, exist_ok=True)
    old = time.time() - 300
    for name, body in (
        ("orders_a.csv", "order_id,amount\nA-1,120.50\nA-2,80.00\n"),
        ("orders_b.csv", "order_id,amount\nB-1,1,200.00\n".replace("1,200.00", "1200.00")),
    ):
        path = os.path.join(drop, name)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(body)
        os.utime(path, (old, old))
    # Still being written: fresh mtime, so the stability window excludes it.
    with open(os.path.join(drop, "orders_c.partial.csv"), "w", encoding="utf-8") as handle:
        handle.write("order_id,amount\nC-1,99")


def build(root: str) -> Workflow:
    fs = FilesystemConnector(root)
    csv = CsvConnector(root)
    workflow = Workflow("file_drop_ingest", description="Ingest a drop directory")

    workflow.add(
        fs.as_task(
            "scan_drop",
            op="watch",
            directory="drop",
            suffix=".csv",
            stable_for=30.0,
            description="list files that have stopped changing",
        )
    )

    @workflow.task("load_rows", depends_on=["scan_drop"])
    def load_rows(ctx: TaskContext) -> Dict[str, Any]:
        """Read every stable file. One unreadable file must not lose the rest."""
        files = ctx.result("scan_drop")["data"]
        rows: List[Dict[str, Any]] = []
        failures: List[str] = []
        for entry in files:
            try:
                result = csv.read(entry["path"], require_columns=["order_id", "amount"])
            except Exception as exc:  # noqa: BLE001 - recorded, not swallowed
                failures.append(f"{entry['name']}: {exc}")
                continue
            for row in result["data"]:
                row["source"] = entry["name"]
                row["amount"] = to_number(row["amount"], default=0.0)
                rows.append(row)
        return {"rows": rows, "files": [f["name"] for f in files], "failures": failures}

    @workflow.task("validate", depends_on=["load_rows"])
    def validate(ctx: TaskContext) -> Dict[str, Any]:
        payload = ctx.result("load_rows", expect=dict)
        total = sum(row["amount"] for row in payload["rows"])
        return {
            "rows": len(payload["rows"]),
            "total": round(total, 2),
            "files": payload["files"],
            "failures": payload["failures"],
        }

    @workflow.task("archive", depends_on=["validate"])
    def archive(ctx: TaskContext) -> List[str]:
        """Move ingested files aside. Safe to repeat."""
        payload = ctx.result("validate", expect=dict)
        moved = []
        for name in payload["files"]:
            result = fs.archive(os.path.join("drop", name), "archive")
            moved.append(
                f"{name} -> {os.path.relpath(result['data'], root)}"
                + (" (already archived)" if result["meta"]["already_archived"] else "")
            )
        return moved

    @workflow.task(
        "index_search",
        depends_on=["validate"],
        on_failure=OnFailure.SKIP,
        description="best effort: a search index is not worth failing the ingest",
    )
    def index_search(ctx: TaskContext) -> str:
        raise ConnectionError("search cluster is not accepting writes")

    return workflow


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="flowforge-drop-") as root:
        seed_drop_directory(os.path.join(root, "drop"))
        workflow = build(root)
        executor = Executor(
            workflow,
            options=ExecutionOptions(max_workers=2),
            state_store=StateStore(os.path.join(root, "runs")),
        )
        state = executor.run()
        print(summarise(state))
        print()
        print(render_gantt(state))
        print()
        print(f"archived: {', '.join(sorted(os.listdir(os.path.join(root, 'archive'))))}")
        print(
            "left in drop (still uploading): "
            f"{sorted(os.listdir(os.path.join(root, 'drop')))}"
        )
        print()
        print("second run, now that the drop directory has been emptied:")
        again = executor.run()
        print(f"  {summarise(again)}")
        empty = again.tasks["validate"]
        print(f"  validate output digest: {empty.output_digest} (zero rows this time)")
        print(f"  index_search: {again.tasks['index_search'].status.value} "
              f"(non-fatal by policy, so the ingest still completed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
