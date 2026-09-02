"""End-to-end: the CLI, the demo, and every shipped example actually run.

These are the tests that catch the embarrassing failures -- an example that
stopped importing, a CLI flag that was renamed, a README command that no longer
exists. They run the real entry points in a subprocess with no network.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from flowforge.cli import main, parse_params
from flowforge.state import StateStore

WORKFLOW = "examples/workflows/nightly_rollup.yaml"


def run_cli(repo_root, *args, expect: int = 0):
    """Invoke the CLI in-process and capture its exit code."""
    cwd = os.getcwd()
    os.chdir(repo_root)
    try:
        code = main(list(args))
    finally:
        os.chdir(cwd)
    assert code == expect, f"flowforge {' '.join(args)} exited {code}, expected {expect}"
    return code


def test_validate_accepts_the_example_workflow(repo_root, capsys):
    run_cli(repo_root, "validate", WORKFLOW)
    out = capsys.readouterr().out
    assert "ok: nightly_rollup" in out
    assert "critical path" in out


def test_validate_rejects_a_broken_workflow(repo_root, tmp_path, capsys):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "name: broken\ntasks:\n  - id: a\n    uses: python:json:dumps\n    depends_on: [ghost]\n",
        encoding="utf-8",
    )
    run_cli(repo_root, "validate", str(bad), expect=1)
    assert "invalid:" in capsys.readouterr().err


def test_graph_renders_levels_and_the_critical_path(repo_root, capsys):
    run_cli(repo_root, "graph", WORKFLOW)
    out = capsys.readouterr().out
    assert "level 0" in out
    assert "critical path: extract -> validate -> rollup -> write_csv -> notify" in out


def test_run_executes_the_workflow_and_writes_state(repo_root, tmp_path, capsys):
    state_dir = str(tmp_path / "runs")
    run_cli(
        repo_root,
        "run",
        WORKFLOW,
        "--param",
        f"out_dir={tmp_path}",
        "--param",
        "run_date=2026-02-17",
        "--workers",
        "2",
        "--state-dir",
        state_dir,
        "--gantt",
        "--metrics",
    )
    out = capsys.readouterr().out
    assert "succeeded" in out
    assert (tmp_path / "rollup.csv").exists()
    payload = json.loads(out[out.index("{") :])
    assert payload["tasks_total"] == 6
    assert payload["success_rate"] == 1.0

    runs = StateStore(state_dir).list_runs("nightly_rollup")
    assert len(runs) == 1
    assert runs[0].status.value == "succeeded"


def test_run_exit_code_2_signals_a_degraded_run(repo_root, tmp_path, capsys):
    """A partially-failed run must not look like success to cron."""
    (tmp_path / "degraded_steps.py").write_text(
        "from flowforge.errors import TransientError\n"
        "def ok(ctx):\n"
        "    return 'done'\n"
        "def best_effort(ctx):\n"
        "    raise TransientError('metrics gateway down')\n",
        encoding="utf-8",
    )
    workflow = tmp_path / "degraded.yaml"
    workflow.write_text(
        "name: degraded\n"
        "tasks:\n"
        "  - id: core\n"
        "    uses: python:degraded_steps:ok\n"
        "  - id: metrics\n"
        "    uses: python:degraded_steps:best_effort\n"
        "    depends_on: [core]\n"
        "    on_failure: skip\n",
        encoding="utf-8",
    )
    run_cli(repo_root, "run", str(workflow), expect=2)
    out = capsys.readouterr().out
    assert "degraded" in out
    assert "root cause" in out


def test_a_non_callable_python_target_is_rejected(repo_root, tmp_path, capsys):
    workflow = tmp_path / "bad_target.yaml"
    workflow.write_text(
        "name: bad_target\n"
        "tasks:\n"
        "  - id: nope\n"
        "    uses: python:flowforge.cli:EXIT_OK\n",
        encoding="utf-8",
    )
    run_cli(repo_root, "run", str(workflow), expect=1)
    assert "not callable" in capsys.readouterr().err


def test_history_lists_recorded_runs(repo_root, tmp_path, capsys):
    state_dir = str(tmp_path / "runs")
    run_cli(
        repo_root, "run", WORKFLOW, "--param", f"out_dir={tmp_path}",
        "--param", "run_date=2026-02-17", "--state-dir", state_dir,
    )
    capsys.readouterr()
    run_cli(repo_root, "history", "--state-dir", state_dir)
    out = capsys.readouterr().out
    assert "nightly_rollup" in out
    assert "succeeded" in out


def test_next_runs_reads_the_schedule_from_the_workflow(repo_root, capsys):
    run_cli(
        repo_root,
        "next-runs",
        "--workflow",
        WORKFLOW,
        "--after",
        "2026-03-27T12:00:00",
        "--count",
        "3",
    )
    out = capsys.readouterr().out
    assert "15 3 * * mon-fri" in out
    # The Friday after 27 March 2026 is skipped: the next weekday run is Monday.
    assert "2026-03-30T03:15:00+01:00" in out


def test_next_runs_from_a_bare_cron_expression(repo_root, capsys):
    run_cli(repo_root, "next-runs", "*/15 * * * *", "--after", "2026-01-01T00:00:00",
            "--count", "2")
    out = capsys.readouterr().out
    assert "2026-01-01T00:15:00+00:00" in out
    assert "2026-01-01T00:30:00+00:00" in out


def test_resume_reruns_only_what_did_not_succeed(repo_root, tmp_path, capsys):
    state_dir = str(tmp_path / "runs")
    workflow = tmp_path / "resumable.yaml"
    workflow.write_text(
        "name: resumable\n"
        "tasks:\n"
        "  - id: first\n"
        "    uses: python:steps_for_test:ok\n"
        "  - id: second\n"
        "    uses: python:steps_for_test:maybe\n"
        "    depends_on: [first]\n",
        encoding="utf-8",
    )
    (tmp_path / "steps_for_test.py").write_text(
        "from flowforge.errors import TransientError\n"
        "def ok(ctx):\n"
        "    return 'first-done'\n"
        "def maybe(ctx):\n"
        "    if not ctx.param('up', False):\n"
        "        raise TransientError('downstream is down')\n"
        "    return ctx.result('first') + '/second-done'\n",
        encoding="utf-8",
    )
    run_cli(repo_root, "run", str(workflow), "--state-dir", state_dir,
            "--run-id", "r1", expect=1)
    capsys.readouterr()

    run_cli(repo_root, "resume", str(workflow), "--state-dir", state_dir,
            "--param", "up=true")
    out = capsys.readouterr().out
    assert "1 task(s) already done, 1 to run" in out
    assert "will run: second" in out
    assert "succeeded" in out


def test_parse_params_reads_json_when_it_can():
    params = parse_params(["a=1", "b=text", "c=[1, 2]", "d=true"])
    assert params == {"a": 1, "b": "text", "c": [1, 2], "d": True}


def test_demo_runs_end_to_end(repo_root):
    result = subprocess.run(
        [sys.executable, "tools/flowforge", "--demo"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert "emails actually sent across both runs: 1" in result.stdout
    assert "upstream_failed" in result.stdout or "skipped" in result.stdout
    assert "root cause" in result.stdout


@pytest.mark.parametrize(
    "script",
    [
        "nightly_report.py",
        "file_drop_ingest.py",
        "api_poll_reconcile.py",
        "resume_after_failure.py",
    ],
)
def test_every_example_runs_offline(repo_root, script):
    path = os.path.join(repo_root, "examples", script)
    assert os.path.exists(path)
    result = subprocess.run(
        [sys.executable, path],
        cwd=os.path.join(repo_root, "examples"),
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, f"{script} failed:\n{result.stderr}"
    assert result.stdout.strip()


def test_readme_documents_commands_that_exist(repo_root):
    """Every ``./tools/flowforge <sub>`` in the README is a real subcommand."""
    with open(os.path.join(repo_root, "README.md"), encoding="utf-8") as handle:
        readme = handle.read()
    known = {"run", "validate", "graph", "resume", "history", "next-runs", "--demo",
             "--version", "--help"}
    for line in readme.splitlines():
        stripped = line.strip()
        if not stripped.startswith("./tools/flowforge"):
            continue
        parts = stripped.split()
        assert len(parts) > 1, stripped
        assert parts[1] in known, f"README references unknown subcommand: {stripped}"
