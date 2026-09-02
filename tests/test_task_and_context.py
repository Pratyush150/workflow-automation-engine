"""The Task abstraction: metadata, typed context access, class-based steps."""

from __future__ import annotations

import pytest

from flowforge import (
    Executor,
    ExponentialBackoff,
    OnFailure,
    Task,
    TaskContext,
    Workflow,
    task,
)
from flowforge.errors import Cancelled, DuplicateTaskError, PermanentError
from flowforge.task import CancelToken, MissingResult, Step


def test_task_metadata_is_carried_through_the_decorator():
    @task("extract", depends_on=["seed"], retry=ExponentialBackoff(max_attempts=3),
          timeout=30, tags=["io", "nightly"], on_failure="skip", idempotent=True)
    def extract(ctx: TaskContext) -> int:
        """Pull the rows."""
        return 1

    assert isinstance(extract, Task)
    assert extract.id == "extract"
    assert extract.depends_on == ("seed",)
    assert extract.retry.max_attempts == 3
    assert extract.timeout == 30
    assert extract.tags == frozenset({"io", "nightly"})
    assert extract.on_failure is OnFailure.SKIP
    assert extract.idempotent is True
    assert extract.description == "Pull the rows."


def test_task_id_defaults_to_the_function_name():
    @task()
    def load_customers(ctx: TaskContext) -> None:
        pass

    assert load_customers.id == "load_customers"


def test_plain_zero_argument_functions_are_supported():
    calls = []
    plain = Task(id="plain", fn=lambda: calls.append("ran"))
    plain(TaskContext(run_id="r", task_id="plain"))
    assert calls == ["ran"]


def test_tasks_are_frozen_and_copied_with_with_():
    original = Task(id="a", fn=lambda ctx: 1)
    modified = original.with_(timeout=5)
    assert original.timeout is None
    assert modified.timeout == 5
    with pytest.raises(Exception):
        original.id = "b"  # type: ignore[misc]


def test_invalid_task_definitions_are_rejected():
    with pytest.raises(ValueError):
        Task(id="", fn=lambda ctx: 1)
    with pytest.raises(TypeError):
        Task(id="a", fn="not callable")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Task(id="a", fn=lambda ctx: 1, timeout=0)


def test_context_result_is_typed_and_explains_a_missing_dependency():
    ctx = TaskContext(run_id="r", task_id="b", results={"a": [1, 2]})
    assert ctx.result("a", expect=list) == [1, 2]
    with pytest.raises(PermanentError) as excinfo:
        ctx.result("a", expect=dict)
    assert "expected 'a' to return dict" in str(excinfo.value)
    with pytest.raises(PermanentError) as excinfo:
        ctx.result("missing")
    assert "depends_on" in str(excinfo.value)


def test_context_params_are_typed_with_actionable_errors():
    ctx = TaskContext(run_id="r", task_id="t", params={"date": "2026-01-01", "n": 3})
    assert ctx.param("date") == "2026-01-01"
    assert ctx.param("nope", "fallback") == "fallback"
    assert ctx.param("n", expect=int) == 3
    with pytest.raises(PermanentError) as excinfo:
        ctx.param("missing")
    assert "missing run parameter" in str(excinfo.value)
    with pytest.raises(PermanentError):
        ctx.param("n", expect=str)


def test_missing_result_placeholder_explains_itself():
    ctx = TaskContext(
        run_id="r",
        task_id="b",
        results={"a": MissingResult("a", "its output was not persisted")},
    )
    with pytest.raises(PermanentError) as excinfo:
        ctx.result("a")
    assert "not persisted" in str(excinfo.value)


def test_cancel_token_is_cooperative():
    token = CancelToken()
    assert token.cancelled is False
    token.raise_if_cancelled()
    token.cancel("timeout after 5s")
    assert token.cancelled is True
    assert token.reason == "timeout after 5s"
    with pytest.raises(Cancelled):
        token.raise_if_cancelled()


def test_time_left_uses_the_run_deadline():
    ctx = TaskContext(run_id="r", task_id="t", deadline=100.0)
    assert ctx.time_left(now=40.0) == 60.0
    assert TaskContext(run_id="r", task_id="t").time_left(now=1.0) is None


class Loader(Step):
    """A class-based step with configuration and state."""

    idempotent = True

    def __init__(self, table: str) -> None:
        self.table = table
        self.rows_written = 0

    def run(self, ctx: TaskContext) -> int:
        rows = ctx.result("extract", expect=list)
        self.rows_written += len(rows)
        return self.rows_written

    def idempotency_key(self, ctx: TaskContext) -> str:
        return f"load:{self.table}:{len(ctx.result('extract'))}"


def test_class_based_step_runs_like_a_function():
    workflow = Workflow("classy")

    @workflow.task("extract")
    def extract(ctx: TaskContext) -> list:
        return [1, 2, 3]

    loader = Loader("customers")
    workflow.add_step(loader, "load", depends_on=["extract"])

    state = Executor(workflow).run()
    assert state.status.value == "succeeded"
    assert loader.rows_written == 3
    assert workflow["load"].idempotent is True
    # The key is computed and recorded even with no store attached, so a run
    # log tells you what key *would* have been used.
    assert state.tasks["load"].idempotency_key == "load:customers:3"


def test_step_idempotency_key_serves_a_second_run_from_the_store(guard):
    workflow = Workflow("classy2")

    @workflow.task("extract")
    def extract(ctx: TaskContext) -> list:
        return [1, 2, 3]

    loader = Loader("customers")
    workflow.add_step(loader, "load", depends_on=["extract"])
    executor = Executor(workflow, idempotency=guard)
    first = executor.run()
    second = executor.run()
    assert first.tasks["load"].idempotency_key == "load:customers:3"
    assert second.tasks["load"].status.value == "cached"
    assert loader.rows_written == 3, "the second run must not load the rows again"


def test_workflow_rejects_duplicate_ids_and_reports_unknown_tasks():
    workflow = Workflow("dupes")
    workflow.add(Task(id="a", fn=lambda ctx: 1))
    with pytest.raises(DuplicateTaskError):
        workflow.add(Task(id="a", fn=lambda ctx: 2))
    with pytest.raises(Exception):
        workflow["nope"]


def test_workflow_lint_flags_a_retrying_non_idempotent_task():
    workflow = Workflow("linted")
    workflow.add(
        Task(id="send", fn=lambda ctx: 1, retry=ExponentialBackoff(max_attempts=3))
    )
    warnings = workflow.lint()
    assert any("not declared idempotent" in warning for warning in warnings)
    assert any("no timeout" in warning for warning in warnings)


def test_workflow_lint_is_quiet_when_the_declaration_is_honest():
    workflow = Workflow("clean")
    workflow.add(
        Task(
            id="fetch",
            fn=lambda ctx: 1,
            retry=ExponentialBackoff(max_attempts=3),
            timeout=10,
            idempotent=True,
        )
    )
    assert workflow.lint() == []
