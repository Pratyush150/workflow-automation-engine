"""CSV transforms with the stdlib ``csv`` module, and a guarded Excel path.

CSV is where most "internal tool" automation lives, and it is where the boring
bugs live too: a BOM on the first header, ``\\r\\n`` turning into blank rows,
Excel writing ``1,234.00`` into a numeric column, an empty file that produces
zero rows and a green run.

This connector handles the first three (``utf-8-sig``, ``newline=""``,
:func:`to_number`) and refuses to hide the fourth: ``read`` reports the row
count in ``meta`` so a downstream check can fail on zero rows rather than
writing an empty report.

``.xlsx`` needs ``openpyxl``. It is optional; without it the CSV half works
normally and the Excel half raises a message telling you what to install.
"""

from __future__ import annotations

import csv
import io
import os
from typing import Any, Callable, Dict, List, Optional, Sequence

from ..errors import ConnectorError, MissingDependency, PermanentError
from ..task import TaskContext
from . import Connector, connector_result

__all__ = ["CsvConnector", "aggregate", "to_number"]

try:  # pragma: no cover - exercised only where openpyxl is installed
    import openpyxl  # type: ignore

    HAVE_OPENPYXL = True
except ImportError:  # pragma: no cover
    openpyxl = None  # type: ignore
    HAVE_OPENPYXL = False


def to_number(value: Any, *, default: Optional[float] = None) -> float:
    """Parse a spreadsheet number: thousands separators, currency, parentheses.

    ``"(1,234.50)"`` is accounting notation for -1234.50 and turns up in every
    export from every finance system. Raises :class:`PermanentError` when the
    value is not a number and no default is given -- a bad cell is not something
    a retry fixes.
    """
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = str(value).strip()
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]
    for junk in ("$", "£", "€", ",", " ", " "):
        text = text.replace(junk, "")
    if not text:
        if default is not None:
            return default
        raise PermanentError("empty value where a number was expected")
    try:
        number = float(text)
    except ValueError:
        if default is not None:
            return default
        raise PermanentError(f"{value!r} is not a number") from None
    return -number if negative else number


def aggregate(
    rows: Sequence[Dict[str, Any]],
    group_by: Sequence[str],
    sums: Sequence[str] = (),
    *,
    count_field: str = "count",
) -> List[Dict[str, Any]]:
    """Group rows and sum numeric columns. Deterministic ordering by key."""
    buckets: Dict[tuple, Dict[str, Any]] = {}
    for row in rows:
        key = tuple(str(row.get(field, "")) for field in group_by)
        bucket = buckets.setdefault(
            key,
            {
                **{field: row.get(field, "") for field in group_by},
                count_field: 0,
                **{field: 0.0 for field in sums},
            },
        )
        bucket[count_field] += 1
        for field in sums:
            bucket[field] += to_number(row.get(field, 0), default=0.0)
    return [buckets[key] for key in sorted(buckets)]


class CsvConnector(Connector):
    """Read, transform and write delimited files."""

    name = "csv"
    #: Reading changes nothing; writing the same rows produces the same file.
    idempotent = True

    def __init__(self, root: str = ".") -> None:
        self.root = os.path.abspath(root)

    def path(self, path: str) -> str:
        return os.path.abspath(os.path.join(self.root, path))

    def run(self, ctx: TaskContext, *, op: str = "read", **kwargs: Any) -> Dict[str, Any]:
        handlers = {"read": self.read, "write": self.write, "transform": self.transform}
        if op not in handlers:
            raise ConnectorError(
                f"unknown csv op {op!r}; expected one of {sorted(handlers)}"
            )
        ctx.cancel.raise_if_cancelled()
        return handlers[op](**kwargs)

    def read(
        self,
        path: str,
        *,
        delimiter: str = ",",
        require_columns: Sequence[str] = (),
        min_rows: int = 0,
    ) -> Dict[str, Any]:
        """Read a CSV into a list of dicts.

        ``utf-8-sig`` strips the byte-order mark Excel writes, which otherwise
        turns the first column name into ``"\\ufeffid"`` and breaks every lookup
        by that name.
        """
        full = self.path(path)
        if not os.path.exists(full):
            raise PermanentError(f"csv file not found: {full}")
        with open(full, "r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, delimiter=delimiter)
            columns = list(reader.fieldnames or [])
            rows = [dict(row) for row in reader]
        missing = [c for c in require_columns if c not in columns]
        if missing:
            raise PermanentError(
                f"{full} is missing required column(s) {missing}; found {columns}"
            )
        if min_rows and len(rows) < min_rows:
            raise PermanentError(
                f"{full} has {len(rows)} row(s), expected at least {min_rows}"
            )
        return connector_result(True, rows, path=full, rows=len(rows), columns=columns)

    def write(
        self,
        path: str,
        rows: Sequence[Dict[str, Any]] = (),
        *,
        columns: Sequence[str] = (),
        delimiter: str = ",",
    ) -> Dict[str, Any]:
        """Write rows atomically. Column order is explicit or taken from row 1."""
        full = self.path(path)
        os.makedirs(os.path.dirname(full) or self.root, exist_ok=True)
        fieldnames = list(columns) or (list(rows[0]) if rows else [])
        buffer = io.StringIO()
        writer = csv.DictWriter(
            buffer, fieldnames=fieldnames, delimiter=delimiter, extrasaction="ignore"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        text = buffer.getvalue()
        tmp = full + ".partial"
        with open(tmp, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(tmp, full)
        return connector_result(
            True, full, path=full, rows=len(rows), columns=fieldnames, bytes=len(text)
        )

    def transform(
        self,
        rows: Sequence[Dict[str, Any]] = (),
        *,
        select: Sequence[str] = (),
        rename: Optional[Dict[str, str]] = None,
        where: Optional[Callable[[Dict[str, Any]], bool]] = None,
        sort_by: Sequence[str] = (),
        numeric: Sequence[str] = (),
    ) -> Dict[str, Any]:
        """Project, rename, filter, coerce and sort, in that order."""
        out: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            if select:
                item = {k: item.get(k, "") for k in select}
            if rename:
                item = {rename.get(k, k): v for k, v in item.items()}
            for column in numeric:
                if column in item:
                    item[column] = to_number(item[column], default=0.0)
            if where is not None and not where(item):
                continue
            out.append(item)
        for column in reversed(list(sort_by)):
            out.sort(key=lambda r, c=column: (r.get(c) is None, r.get(c)))
        return connector_result(True, out, rows=len(out), dropped=len(rows) - len(out))

    # ------------------------------------------------------------------- excel

    def read_xlsx(self, path: str, *, sheet: Optional[str] = None) -> Dict[str, Any]:
        """Read the first sheet of an ``.xlsx`` file. Needs ``openpyxl``."""
        if not HAVE_OPENPYXL:  # pragma: no cover - depends on the environment
            raise MissingDependency("openpyxl", "reading .xlsx files")
        book = openpyxl.load_workbook(self.path(path), read_only=True, data_only=True)
        worksheet = book[sheet] if sheet else book.worksheets[0]
        values = list(worksheet.values)
        if not values:
            return connector_result(True, [], rows=0, columns=[])
        header = [str(c) if c is not None else "" for c in values[0]]
        rows = [dict(zip(header, row)) for row in values[1:]]
        return connector_result(True, rows, rows=len(rows), columns=header)

    def key_material(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        material = dict(kwargs)
        material.pop("where", None)  # a callable has no stable identity
        return material
