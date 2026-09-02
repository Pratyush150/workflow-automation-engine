"""The YAML subset loader and workflow schema validation."""

from __future__ import annotations

import os

import pytest

from flowforge.dsl import Node, load_workflow, load_yaml, validate_spec
from flowforge.errors import ValidationError
from flowforge.connectors.mock import RecordingConnector

MINIMAL = """
name: minimal
tasks:
  - id: only
    uses: python:json:dumps
"""


def test_scalars_lists_and_nesting():
    text = """
name: demo
deadline: 300
enabled: true
missing: null
ratio: 0.25
defaults:
  retry: {attempts: 3, strategy: fixed}
  timeout: 30
tags: [a, b, c]
tasks:
  - id: one
    uses: python:json:dumps
    depends_on: []
"""
    spec = load_yaml(text)
    assert spec["name"] == "demo"
    assert spec["deadline"] == 300
    assert spec["enabled"] is True
    assert spec["missing"] is None
    assert spec["ratio"] == 0.25
    assert spec["tags"] == ["a", "b", "c"]
    assert spec["defaults"]["retry"]["attempts"] == 3
    assert spec["tasks"][0]["id"] == "one"


def test_comments_quotes_and_block_scalars():
    text = """
# leading comment
name: "quoted name"   # trailing comment
description: |
  first line
  second line
schedule: {cron: "0 3 * * *", tz: Europe/London}
tasks:
  - id: a          # inline comment
    uses: python:json:dumps
"""
    spec = load_yaml(text)
    assert spec["name"] == "quoted name"
    assert spec["description"] == "first line\nsecond line"
    assert spec["schedule"]["cron"] == "0 3 * * *"
    assert spec["tasks"][0]["id"] == "a"


def test_line_numbers_are_recorded_for_every_key():
    spec = load_yaml(MINIMAL, source="wf.yaml")
    assert isinstance(spec, Node)
    assert spec.line_of("name") == 2
    assert spec.line_of("tasks") == 3
    assert spec["tasks"][0].line_of("uses") == 5


def test_unknown_key_names_the_line_and_suggests_the_right_one():
    text = MINIMAL.replace("uses:", "use:")
    with pytest.raises(ValidationError) as excinfo:
        load_workflow(text, source="wf.yaml")
    error = excinfo.value
    assert error.key == "use"
    assert error.line == 5
    assert "did you mean 'uses'?" in str(error)
    assert "wf.yaml" in str(error)


def test_typo_in_depends_on_is_caught_with_a_suggestion():
    text = """
name: typo
tasks:
  - id: a
    uses: python:json:dumps
  - id: b
    uses: python:json:dumps
    depends: [a]
"""
    with pytest.raises(ValidationError) as excinfo:
        load_workflow(text, source="wf.yaml")
    assert "did you mean 'depends_on'?" in str(excinfo.value)
    assert excinfo.value.line == 8


def test_missing_name_and_empty_tasks_are_rejected():
    with pytest.raises(ValidationError) as excinfo:
        validate_spec(load_yaml("tasks:\n  - id: a\n    uses: python:json:dumps\n"))
    assert "needs a 'name'" in str(excinfo.value)
    with pytest.raises(ValidationError) as excinfo:
        validate_spec(load_yaml("name: x\ntasks: []\n"))
    assert "non-empty 'tasks'" in str(excinfo.value)


def test_duplicate_task_id_points_at_the_first_definition():
    text = """
name: dupes
tasks:
  - id: a
    uses: python:json:dumps
  - id: a
    uses: python:json:dumps
"""
    with pytest.raises(ValidationError) as excinfo:
        load_workflow(text, source="wf.yaml")
    assert "duplicate task id 'a'" in str(excinfo.value)
    assert "first defined on line 4" in str(excinfo.value)


def test_dependency_on_an_undefined_task_is_reported_with_its_line():
    text = """
name: dangling
tasks:
  - id: a
    uses: python:json:dumps
    depends_on: [ghost]
"""
    with pytest.raises(ValidationError) as excinfo:
        load_workflow(text, source="wf.yaml")
    assert "depends on 'ghost'" in str(excinfo.value)
    assert excinfo.value.line == 6


def test_bad_on_failure_lists_the_valid_values():
    text = MINIMAL.replace("uses: python:json:dumps", "uses: python:json:dumps\n    on_failure: explode")
    with pytest.raises(ValidationError) as excinfo:
        load_workflow(text)
    assert "'fail'" in str(excinfo.value) and "'skip'" in str(excinfo.value)


def test_unresolvable_python_target_says_what_is_wrong():
    text = MINIMAL.replace("python:json:dumps", "python:json:no_such_function")
    with pytest.raises(ValidationError) as excinfo:
        load_workflow(text)
    assert "no attribute 'no_such_function'" in str(excinfo.value)

    text = MINIMAL.replace("python:json:dumps", "python:not_a_real_module:f")
    with pytest.raises(ValidationError) as excinfo:
        load_workflow(text)
    assert "cannot import module" in str(excinfo.value)


def test_uses_must_name_a_known_scheme():
    text = MINIMAL.replace("python:json:dumps", "magic:do_it")
    with pytest.raises(ValidationError) as excinfo:
        load_workflow(text)
    assert "'python:' or 'connector:'" in str(excinfo.value)


def test_connector_steps_are_wired_with_their_arguments():
    recorder = RecordingConnector(value={"done": True})
    text = """
name: connectors
tasks:
  - id: step
    uses: connector:mock
    with: {payload: 42}
    idempotency_key: auto
"""
    workflow = load_workflow(text, connectors={"mock": recorder})
    assert workflow["step"].idempotency_key is not None
    assert "connector:mock" in workflow["step"].tags

    from flowforge import Executor

    state = Executor(workflow).run()
    assert state.status.value == "succeeded"
    assert recorder.calls[0]["payload"] == 42


def test_missing_connector_is_reported_by_name():
    text = """
name: connectors
tasks:
  - id: step
    uses: connector:nowhere
"""
    with pytest.raises(ValidationError) as excinfo:
        load_workflow(text, source="wf.yaml")
    assert "connector 'nowhere'" in str(excinfo.value)


def test_resolve_false_validates_without_importing_anything():
    text = MINIMAL.replace("python:json:dumps", "python:definitely_not_installed:f")
    workflow = load_workflow(text, resolve=False)
    assert workflow.ids == ["only"]
    workflow.validate()


def test_idempotency_key_template_is_rendered_from_run_params():
    text = """
name: keyed
tasks:
  - id: notify
    uses: python:json:dumps
    idempotency_key: "notify:{param:run_date}"
"""
    workflow = load_workflow(text)
    from flowforge import TaskContext

    ctx = TaskContext(run_id="r", task_id="notify", params={"run_date": "2026-02-17"})
    assert workflow["notify"].key_for(ctx) == "notify:2026-02-17"
    with pytest.raises(ValidationError):
        workflow["notify"].key_for(TaskContext(run_id="r", task_id="notify"))


def test_auto_key_on_a_python_step_is_rejected_with_guidance():
    text = MINIMAL.replace(
        "uses: python:json:dumps", "uses: python:json:dumps\n    idempotency_key: auto"
    )
    with pytest.raises(ValidationError) as excinfo:
        load_workflow(text)
    assert "needs a connector step" in str(excinfo.value)


def test_defaults_apply_to_tasks_that_do_not_override_them():
    text = """
name: defaults
defaults:
  retry: {attempts: 4, strategy: fixed, delay: 1}
  timeout: 12
tasks:
  - id: a
    uses: python:json:dumps
  - id: b
    uses: python:json:dumps
    retry: 1
    timeout: 3
"""
    workflow = load_workflow(text)
    assert workflow["a"].retry.max_attempts == 4
    assert workflow["a"].timeout == 12
    assert workflow["b"].retry.max_attempts == 1
    assert workflow["b"].timeout == 3


def test_bad_indentation_is_a_parse_error_with_a_line():
    text = "name: x\ntasks:\n  - id: a\n      uses: python:json:dumps\n"
    with pytest.raises(ValidationError) as excinfo:
        load_yaml(text, source="wf.yaml")
    assert excinfo.value.line == 4


def test_the_shipped_example_workflow_loads_and_validates(repo_root):
    path = os.path.join(repo_root, "examples", "workflows", "nightly_rollup.yaml")
    spec = load_yaml(open(path, encoding="utf-8").read(), source=path)
    validate_spec(spec, path)
    assert spec["name"] == "nightly_rollup"
    assert spec["schedule"]["tz"] == "Europe/London"
    assert [t["id"] for t in spec["tasks"]] == [
        "extract",
        "validate",
        "rollup",
        "write_csv",
        "checksum",
        "notify",
    ]
