"""Command line interface: ``flowforge run|validate|graph|resume|history|next-runs``.

Exit codes are part of the contract, because whatever runs this (cron, systemd,
a CI job) can only see the exit code:

===  ===========================================================
 0   the run succeeded, every task included
 1   the run failed, or the file did not validate
 2   the run finished **degraded** -- something failed under a
     non-fatal policy, so the output exists but is incomplete
===  ===========================================================

Code 2 exists because "partially worked" is the state that silently rots. A
workflow that returns 0 when a branch was skipped will be quietly broken for
months.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Sequence

from .connectors.csv_excel import CsvConnector
from .connectors.email_smtp import SmtpConnector
from .connectors.filesystem import FilesystemConnector
from .connectors.http import HttpConnector
from .connectors.shell import ShellConnector
from .dsl import load_workflow_file, load_yaml, schedule_of
from .errors import FlowForgeError, ValidationError
from .executor import ExecutionOptions, Executor
from .idempotency import IdempotencyGuard, JsonFileStore
from .observability import RunLogger, explain_failure, metrics, render_gantt, summarise
from .schedule import CronSchedule, next_runs
from .state import RunStatus, StateStore
from .workflow import Workflow

__all__ = ["main"]

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_DEGRADED = 2


def default_connectors(root: str) -> Dict[str, Any]:
    """Connectors available to a YAML workflow without extra wiring.

    ``email`` is dry-run by default. A CLI that mails people the first time you
    try it is a CLI nobody trusts twice.
    """
    return {
        "csv": CsvConnector(root),
        "filesystem": FilesystemConnector(root),
        "shell": ShellConnector(cwd=root),
        "http": HttpConnector(),
        "email": SmtpConnector(dry_run=True),
    }


def load_target(target: str, *, resolve: bool = True, root: str = ".") -> Workflow:
    """Load a workflow from a YAML file or a ``python:module:attribute`` target."""
    if target.startswith("python:"):
        body = target.split(":", 1)[1]
        module_name, _, attribute = body.rpartition(":")
        if not module_name or not attribute:
            raise ValidationError(
                f"{target!r} must look like python:module:attribute", key="workflow"
            )
        module = importlib.import_module(module_name)
        obj = getattr(module, attribute, None)
        if obj is None:
            raise ValidationError(
                f"{module_name!r} has no attribute {attribute!r}", key="workflow"
            )
        workflow = obj() if callable(obj) and not isinstance(obj, Workflow) else obj
        if not isinstance(workflow, Workflow):
            raise ValidationError(
                f"{target!r} is a {type(workflow).__name__}, not a Workflow",
                key="workflow",
            )
        return workflow
    if not os.path.exists(target):
        raise ValidationError(f"no such workflow file: {target}", key="workflow")
    # A workflow file's own directory goes on sys.path, so "python:steps:extract"
    # resolves against the file you are running rather than against wherever the
    # shell happened to be.
    directory = os.path.dirname(os.path.abspath(target))
    if directory not in sys.path:
        sys.path.insert(0, directory)
    return load_workflow_file(
        target, connectors=default_connectors(root), resolve=resolve
    )


def parse_params(pairs: Sequence[str]) -> Dict[str, Any]:
    """``k=v`` pairs. Values that parse as JSON become JSON."""
    params: Dict[str, Any] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValidationError(
                f"parameter {pair!r} must look like key=value", key="param"
            )
        key, _, value = pair.partition("=")
        try:
            params[key.strip()] = json.loads(value)
        except ValueError:
            params[key.strip()] = value
    return params


def build_executor(
    workflow: Workflow, args: argparse.Namespace
) -> Executor:
    store = StateStore(args.state_dir) if args.state_dir else None
    guard = None
    if args.idempotency_store:
        guard = IdempotencyGuard(
            JsonFileStore(args.idempotency_store), on_ambiguous=args.on_ambiguous
        )
    return Executor(
        workflow,
        options=ExecutionOptions(
            max_workers=args.workers,
            deadline=args.deadline,
            default_timeout=args.task_timeout,
            fail_fast=args.fail_fast,
        ),
        state_store=store,
        idempotency=guard,
        logger=RunLogger(enabled=args.json_logs),
    )


def exit_code_for(status: RunStatus) -> int:
    if status is RunStatus.SUCCEEDED:
        return EXIT_OK
    if status is RunStatus.DEGRADED:
        return EXIT_DEGRADED
    return EXIT_FAILED


def _print_outcome(state: Any, workflow: Workflow, args: argparse.Namespace) -> None:
    print(summarise(state))
    if args.gantt:
        print()
        print(render_gantt(state))
    if state.status is not RunStatus.SUCCEEDED:
        print()
        print(explain_failure(state, workflow.dag()))
    if args.metrics:
        print()
        print(json.dumps(metrics(state).to_dict(), indent=2, default=str))


# ------------------------------------------------------------------- commands


def cmd_run(args: argparse.Namespace) -> int:
    workflow = load_target(args.workflow, root=args.root)
    executor = build_executor(workflow, args)
    state = executor.run(parse_params(args.param), run_id=args.run_id)
    _print_outcome(state, workflow, args)
    return exit_code_for(state.status)


def cmd_resume(args: argparse.Namespace) -> int:
    if not args.state_dir:
        print("resume needs --state-dir (that is where run state lives)", file=sys.stderr)
        return EXIT_FAILED
    workflow = load_target(args.workflow, root=args.root)
    store = StateStore(args.state_dir)
    if args.run_id:
        previous = store.load(args.run_id, workflow.name)
    else:
        previous = store.latest_failed(workflow.name) or store.latest(workflow.name)
    if previous is None:
        print(f"no previous run of {workflow.name!r} in {args.state_dir}", file=sys.stderr)
        return EXIT_FAILED
    done = previous.completed_tasks()
    todo = previous.resume_from(workflow.ids)
    print(
        f"resuming {previous.run_id} ({previous.status.value}): "
        f"{len(done)} task(s) already done, {len(todo)} to run"
    )
    if todo:
        print(f"  will run: {', '.join(todo)}")
    executor = build_executor(workflow, args)
    state = executor.run(parse_params(args.param), resume=previous)
    _print_outcome(state, workflow, args)
    return exit_code_for(state.status)


def cmd_validate(args: argparse.Namespace) -> int:
    try:
        workflow = load_target(args.workflow, resolve=not args.no_resolve, root=args.root)
        workflow.validate()
    except (FlowForgeError, ImportError) as exc:
        print(f"invalid: {exc}", file=sys.stderr)
        return EXIT_FAILED
    warnings = workflow.lint()
    print(
        f"ok: {workflow.name} -- {len(workflow)} task(s), "
        f"{len(workflow.levels())} level(s), "
        f"critical path {' -> '.join(workflow.dag().critical_path())}"
    )
    for warning in warnings:
        print(f"warning: {warning}")
    isolated = workflow.dag().isolated_nodes()
    if isolated and len(workflow) > 1:
        print(f"warning: unconnected task(s): {', '.join(isolated)}")
    return EXIT_OK


def cmd_graph(args: argparse.Namespace) -> int:
    workflow = load_target(args.workflow, resolve=not args.no_resolve, root=args.root)
    workflow.validate()
    print(f"{workflow.name}: {len(workflow)} tasks")
    if workflow.description:
        print(workflow.description)
    print()
    print(workflow.render())
    print()
    graph = workflow.dag()
    print(f"critical path: {' -> '.join(graph.critical_path())}")
    print(f"parallel levels: {[len(level) for level in graph.levels()]}")
    return EXIT_OK


def cmd_history(args: argparse.Namespace) -> int:
    store = StateStore(args.state_dir or ".flowforge/runs")
    runs = store.list_runs(args.workflow_name)
    if not runs:
        print(f"no runs recorded in {store.directory}")
        return EXIT_OK
    header = f"{'run_id':<26} {'workflow':<20} {'status':<10} {'tasks':>6} {'dur':>8}  started"
    print(header)
    print("-" * len(header))
    for state in runs[: args.limit]:
        counts = f"{len(state.succeeded)}/{len(state.tasks)}"
        duration = f"{state.duration:.2f}s" if state.duration else "-"
        print(
            f"{state.run_id:<26} {state.workflow:<20} {state.status.value:<10} "
            f"{counts:>6} {duration:>8}  {state.started_at or '-'}"
        )
    return EXIT_OK


def cmd_next_runs(args: argparse.Namespace) -> int:
    if args.workflow:
        spec = load_yaml(open(args.workflow, encoding="utf-8").read(), source=args.workflow)
        schedule = schedule_of(spec)
        if schedule is None:
            print(f"{args.workflow} has no 'schedule:' block", file=sys.stderr)
            return EXIT_FAILED
    elif args.expression:
        schedule = CronSchedule(args.expression, args.tz)
    else:
        print("give a cron expression or --workflow", file=sys.stderr)
        return EXIT_FAILED
    start = (
        datetime.fromisoformat(args.after)
        if args.after
        else datetime.now(timezone.utc)
    )
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if isinstance(schedule, CronSchedule):
        print(schedule.describe())
    for moment in next_runs(schedule, start, args.count):
        print(f"  {moment.isoformat()}  ({moment.strftime('%a %d %b %Y %H:%M %Z')})")
    return EXIT_OK


def cmd_demo(args: argparse.Namespace) -> int:
    from .demo import run_demo

    _first, second = run_demo(verbose=args.json_logs)
    return exit_code_for(second.status) if args.strict else EXIT_OK


# --------------------------------------------------------------------- parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="flowforge",
        description=(
            "Run and inspect flowforge workflows. Try: flowforge --demo"
        ),
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="run a self-contained demo workflow (no network, no config)",
    )
    parser.add_argument("--strict", action="store_true", help="let --demo set the exit code")
    parser.add_argument("--version", action="store_true", help="print the version")
    sub = parser.add_subparsers(dest="command")

    def common(target: argparse.ArgumentParser, *, workflow: bool = True) -> None:
        if workflow:
            target.add_argument("workflow", help="workflow.yaml or python:module:attribute")
        target.add_argument("--root", default=".", help="root directory for connectors")
        target.add_argument(
            "--state-dir", default="", help="directory for durable run state"
        )
        target.add_argument("--json-logs", action="store_true", help="structured logs to stderr")

    def run_like(target: argparse.ArgumentParser) -> None:
        target.add_argument("--param", action="append", default=[], metavar="K=V")
        target.add_argument("--workers", type=int, default=1)
        target.add_argument("--deadline", type=float, default=None, help="run budget, seconds")
        target.add_argument("--task-timeout", type=float, default=None)
        target.add_argument("--fail-fast", action="store_true")
        target.add_argument("--gantt", action="store_true", help="print the run timeline")
        target.add_argument("--metrics", action="store_true", help="print metrics as JSON")
        target.add_argument("--idempotency-store", default="", metavar="PATH")
        target.add_argument(
            "--on-ambiguous",
            default="rerun",
            choices=["rerun", "skip", "error"],
            help="what to do with a key left in-flight by a killed run",
        )
        target.add_argument("--run-id", default=None)

    p_run = sub.add_parser("run", help="execute a workflow")
    common(p_run)
    run_like(p_run)
    p_run.set_defaults(func=cmd_run)

    p_resume = sub.add_parser("resume", help="re-run only what did not succeed")
    common(p_resume)
    run_like(p_resume)
    p_resume.set_defaults(func=cmd_resume)

    p_validate = sub.add_parser("validate", help="check a workflow file")
    common(p_validate)
    p_validate.add_argument("--no-resolve", action="store_true", help="skip importing targets")
    p_validate.set_defaults(func=cmd_validate)

    p_graph = sub.add_parser("graph", help="render the DAG as ASCII")
    common(p_graph)
    p_graph.add_argument("--no-resolve", action="store_true")
    p_graph.set_defaults(func=cmd_graph)

    p_history = sub.add_parser("history", help="list recorded runs")
    common(p_history, workflow=False)
    p_history.add_argument("--workflow-name", default=None)
    p_history.add_argument("--limit", type=int, default=20)
    p_history.set_defaults(func=cmd_history)

    p_next = sub.add_parser("next-runs", help="show upcoming fire times")
    p_next.add_argument("expression", nargs="?", help="a 5-field cron expression")
    p_next.add_argument("--workflow", help="read the schedule from a workflow file")
    p_next.add_argument("--tz", default="UTC")
    p_next.add_argument("--count", type=int, default=5)
    p_next.add_argument("--after", default="", help="ISO timestamp to start from")
    p_next.set_defaults(func=cmd_next_runs)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.version:
        from . import __version__

        print(f"flowforge {__version__}")
        return EXIT_OK
    if args.demo:
        args.json_logs = getattr(args, "json_logs", False)
        return cmd_demo(args)
    if not getattr(args, "func", None):
        parser.print_help()
        return EXIT_OK
    try:
        return int(args.func(args))
    except FlowForgeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_FAILED
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        print("interrupted", file=sys.stderr)
        return EXIT_FAILED


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
