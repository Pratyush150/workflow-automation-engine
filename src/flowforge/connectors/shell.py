"""Run a subprocess with a timeout and captured output.

A subprocess is the one kind of step this engine can actually kill, so the
timeout here is real rather than cooperative. ``subprocess.run`` is given the
timeout; on expiry the child is killed and the step raises.

Defaults are deliberate:

* ``shell=False``. The command is a list. String commands go through a shell,
  and a filename with a space in it becomes a security incident.
* Output is captured, truncated for the log, and returned in full to the task.
  An automation step that prints to a terminal nobody is watching has not
  logged anything.
* A non-zero exit code raises. ``check=False`` opts out when a non-zero code is
  meaningful (``grep``, ``diff``).
"""

from __future__ import annotations

import os
import shlex
import subprocess
from typing import Any, Dict, Mapping, Optional, Sequence, Union

from ..errors import ConnectorError, TaskTimeout, TransientError
from ..task import TaskContext
from . import Connector, connector_result

__all__ = ["ShellConnector"]


class ShellConnector(Connector):
    """Run an external command."""

    name = "shell"
    #: A command can be anything. Declaring it idempotent is the caller's call.
    idempotent = False

    def __init__(
        self,
        *,
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        timeout: float = 60.0,
        inherit_env: bool = True,
        idempotent: bool = False,
        max_output_chars: int = 100_000,
    ) -> None:
        self.cwd = cwd
        self.env = dict(env or {})
        self.timeout = timeout
        self.inherit_env = inherit_env
        self.idempotent = idempotent
        self.max_output_chars = max_output_chars

    def run(
        self,
        ctx: TaskContext,
        *,
        command: Union[str, Sequence[str]] = (),
        cwd: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        timeout: Optional[float] = None,
        check: bool = True,
        retry_exit_codes: Sequence[int] = (),
        stdin: str = "",
    ) -> Dict[str, Any]:
        argv = shlex.split(command) if isinstance(command, str) else list(command)
        if not argv:
            raise ConnectorError("shell connector called with an empty command")
        ctx.cancel.raise_if_cancelled()
        environment = dict(os.environ) if self.inherit_env else {}
        environment.update(self.env)
        environment.update(env or {})
        environment.setdefault("FLOWFORGE_RUN_ID", ctx.run_id)
        environment.setdefault("FLOWFORGE_TASK_ID", ctx.task_id)
        limit = timeout or self.timeout
        try:
            completed = subprocess.run(  # noqa: S603 - argv list, shell=False
                argv,
                cwd=cwd or self.cwd,
                env=environment,
                input=stdin,
                capture_output=True,
                text=True,
                timeout=limit,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise TaskTimeout(
                f"command {argv[0]!r} exceeded {limit:g}s and was killed"
            ) from exc
        except FileNotFoundError as exc:
            raise ConnectorError(f"command not found: {argv[0]!r}") from exc
        stdout = completed.stdout[: self.max_output_chars]
        stderr = completed.stderr[: self.max_output_chars]
        if completed.returncode != 0:
            tail = (stderr or stdout).strip().splitlines()[-3:]
            message = (
                f"{argv[0]!r} exited {completed.returncode}: "
                + " | ".join(tail)
            )
            if completed.returncode in tuple(retry_exit_codes):
                raise TransientError(message)
            if check:
                raise ConnectorError(message)
        return connector_result(
            completed.returncode == 0,
            {"stdout": stdout, "stderr": stderr},
            returncode=completed.returncode,
            command=argv,
        )

    def key_material(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return {"command": kwargs.get("command"), "cwd": kwargs.get("cwd") or self.cwd}
