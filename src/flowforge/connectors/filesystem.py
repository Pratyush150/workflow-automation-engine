"""Filesystem steps: watch a drop directory, move, archive, write atomically.

The failure mode this connector is built around is the **half-written file**.
A partner uploads a 40 MB CSV; your watcher sees it 200 ms in, reads 6 MB, and
happily processes a truncated file. Nobody notices for a week.

So :meth:`FilesystemConnector.watch` only reports a file whose size and mtime
have been unchanged for ``stable_for`` seconds. That is not a guarantee -- a
writer that stalls mid-upload for longer than the window will still fool it --
and the honest fix is for the producer to write to a temp name and rename. The
stability check is what you do when you do not control the producer.

Archiving is written to be safe to repeat: if the source is gone and the
destination already exists, the move already happened, and the step reports
``already_archived`` instead of failing a re-run.
"""

from __future__ import annotations

import os
import shutil
import time
from typing import Any, Dict, List, Optional

from ..errors import ConnectorError
from ..task import TaskContext
from . import Connector, connector_result

__all__ = ["FilesystemConnector"]


class FilesystemConnector(Connector):
    """Local filesystem operations, rooted at a directory."""

    name = "filesystem"
    #: Every operation here is written to be safe to repeat.
    idempotent = True

    def __init__(self, root: str = ".") -> None:
        self.root = os.path.abspath(root)

    def path(self, *parts: str) -> str:
        """Resolve a path inside the root, refusing to escape it."""
        candidate = os.path.abspath(os.path.join(self.root, *parts))
        if candidate != self.root and not candidate.startswith(self.root + os.sep):
            raise ConnectorError(
                f"path {candidate!r} escapes the connector root {self.root!r}"
            )
        return candidate

    def run(self, ctx: TaskContext, *, op: str = "watch", **kwargs: Any) -> Dict[str, Any]:
        """Dispatch to ``watch``, ``move``, ``archive``, ``read`` or ``write``."""
        handlers = {
            "watch": self.watch,
            "move": self.move,
            "archive": self.archive,
            "read": self.read,
            "write": self.write,
        }
        if op not in handlers:
            raise ConnectorError(
                f"unknown filesystem op {op!r}; expected one of {sorted(handlers)}"
            )
        ctx.cancel.raise_if_cancelled()
        return handlers[op](**kwargs)

    # --------------------------------------------------------------- operations

    def watch(
        self,
        directory: str = "",
        *,
        suffix: str = "",
        stable_for: float = 0.0,
        now: Optional[float] = None,
        limit: int = 0,
    ) -> Dict[str, Any]:
        """List files that look finished.

        ``stable_for`` is in seconds; ``now`` is injectable so tests do not sleep.
        """
        target = self.path(directory) if directory else self.root
        if not os.path.isdir(target):
            return connector_result(True, [], directory=target, skipped="missing")
        moment = time.time() if now is None else now
        ready: List[Dict[str, Any]] = []
        unstable: List[str] = []
        for name in sorted(os.listdir(target)):
            full = os.path.join(target, name)
            if not os.path.isfile(full):
                continue
            if suffix and not name.endswith(suffix):
                continue
            stat = os.stat(full)
            age = moment - stat.st_mtime
            if stable_for and age < stable_for:
                unstable.append(name)
                continue
            ready.append({"path": full, "name": name, "size": stat.st_size})
        if limit:
            ready = ready[:limit]
        return connector_result(
            True, ready, directory=target, count=len(ready), unstable=unstable
        )

    def move(self, source: str, destination: str, *, overwrite: bool = False) -> Dict[str, Any]:
        """Move a file, creating the destination directory."""
        src = self.path(source)
        dst = self.path(destination)
        os.makedirs(os.path.dirname(dst) or self.root, exist_ok=True)
        if not os.path.exists(src):
            if os.path.exists(dst):
                return connector_result(True, dst, already_moved=True)
            raise ConnectorError(f"cannot move {src!r}: it does not exist")
        if os.path.exists(dst) and not overwrite:
            raise ConnectorError(f"refusing to overwrite {dst!r}")
        shutil.move(src, dst)
        return connector_result(True, dst, already_moved=False)

    def archive(
        self,
        source: str,
        archive_dir: str = "archive",
        *,
        subdirectory: str = "",
    ) -> Dict[str, Any]:
        """Move a processed file into an archive directory.

        Repeating this after a crash is safe: if the source is gone and the
        destination is there, the previous run finished the job.
        """
        src = self.path(source)
        name = os.path.basename(src)
        parts = [archive_dir]
        if subdirectory:
            parts.append(subdirectory)
        parts.append(name)
        dst = self.path(*parts)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if not os.path.exists(src) and os.path.exists(dst):
            return connector_result(True, dst, already_archived=True)
        if not os.path.exists(src):
            raise ConnectorError(f"cannot archive {src!r}: it does not exist")
        shutil.move(src, dst)
        return connector_result(True, dst, already_archived=False)

    def read(self, path: str, *, encoding: str = "utf-8") -> Dict[str, Any]:
        full = self.path(path)
        with open(full, "r", encoding=encoding) as handle:
            text = handle.read()
        return connector_result(True, text, path=full, chars=len(text))

    def write(
        self, path: str, text: str = "", *, encoding: str = "utf-8", atomic: bool = True
    ) -> Dict[str, Any]:
        """Write a file. Atomic by default: temp file plus ``os.replace``.

        A workflow that dies while writing its output should leave the previous
        output intact, not a half-written file that the next step happily reads.
        """
        full = self.path(path)
        os.makedirs(os.path.dirname(full) or self.root, exist_ok=True)
        if not atomic:
            with open(full, "w", encoding=encoding) as handle:
                handle.write(text)
            return connector_result(True, full, atomic=False, chars=len(text))
        tmp = full + ".partial"
        with open(tmp, "w", encoding=encoding) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, full)
        return connector_result(True, full, atomic=True, chars=len(text))

    def key_material(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in kwargs.items() if k != "now"}
