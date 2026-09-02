"""Define workflows in YAML as well as in Python.

Two design decisions worth stating.

**The loader is ours, and it parses a subset.** flowforge has no required
dependencies, and a workflow file is not the place for YAML's exotic corners
(anchors, aliases, custom tags, implicit type coercion of ``NO`` to ``False``).
The subset here is mappings, sequences, scalars, inline ``[a, b]`` and
``{k: v}``, ``|`` block strings, and comments. If you would rather use real
YAML, ``pip install PyYAML`` and pass ``loader="pyyaml"``.

**Errors name the key and the line.** A workflow that fails to load at 03:00
with ``KeyError: 'depends'`` is a bad night. Every validation error here says
what was wrong, which key it was on, and which line of which file it came
from -- including "did you mean ``depends_on``?" for a near-miss key, because
that typo is the single most common way one of these files breaks.
"""

from __future__ import annotations

import difflib
import importlib
import os
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from .errors import ValidationError
from .retry import policy_from_spec
from .schedule import Schedule, schedule_from_spec
from .task import OnFailure, Task, TaskContext
from .workflow import Workflow

__all__ = ["Node", "load_workflow", "load_workflow_file", "load_yaml", "validate_spec"]


class Node(dict):
    """A mapping that remembers which line each key came from."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.lines: Dict[str, int] = {}
        self.line: int = 0

    def line_of(self, key: str) -> Optional[int]:
        return self.lines.get(key, self.line or None)


# --------------------------------------------------------------------- scanner


def _strip_comment(text: str) -> str:
    """Remove a trailing ``#`` comment that is not inside quotes."""
    out: List[str] = []
    quote = ""
    for i, char in enumerate(text):
        if quote:
            out.append(char)
            if char == quote:
                quote = ""
            continue
        if char in "\"'":
            quote = char
            out.append(char)
            continue
        if char == "#" and (i == 0 or text[i - 1] in " \t"):
            break
        out.append(char)
    return "".join(out).rstrip()


def _tokenize(text: str) -> List[Tuple[int, str, int]]:
    tokens: List[Tuple[int, str, int]] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.rstrip("\n").replace("\t", "    ")
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        content = _strip_comment(line)
        if not content.strip():
            continue
        indent = len(content) - len(content.lstrip(" "))
        tokens.append((indent, content.strip(), number))
    return tokens


def _parse_scalar(text: str, line: int, source: str) -> Any:
    value = text.strip()
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        return value[1:-1]
    if value.startswith("'") and value.endswith("'") and len(value) >= 2:
        return value[1:-1]
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part, line, source) for part in _split_top(inner)]
    if value.startswith("{") and value.endswith("}"):
        inner = value[1:-1].strip()
        node = Node()
        node.line = line
        if not inner:
            return node
        for part in _split_top(inner):
            key, sep, rest = part.partition(":")
            if not sep:
                raise ValidationError(
                    f"inline mapping entry {part!r} is missing a ':'",
                    line=line,
                    source=source,
                )
            node[key.strip()] = _parse_scalar(rest, line, source)
            node.lines[key.strip()] = line
        return node
    lowered = value.lower()
    if lowered in ("true", "yes"):
        return True
    if lowered in ("false", "no"):
        return False
    if lowered in ("null", "~", ""):
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def _split_top(text: str) -> List[str]:
    """Split on commas that are not inside brackets or quotes."""
    parts: List[str] = []
    depth = 0
    quote = ""
    current: List[str] = []
    for char in text:
        if quote:
            current.append(char)
            if char == quote:
                quote = ""
            continue
        if char in "\"'":
            quote = char
        elif char in "[{":
            depth += 1
        elif char in "]}":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    if current:
        parts.append("".join(current).strip())
    return [p for p in parts if p]


def _parse_block(
    tokens: List[Tuple[int, str, int]], index: int, indent: int, source: str
) -> Tuple[Any, int]:
    if index < len(tokens) and tokens[index][1].startswith("- "):
        return _parse_sequence(tokens, index, indent, source)
    return _parse_mapping(tokens, index, indent, source)


def _parse_mapping(
    tokens: List[Tuple[int, str, int]], index: int, indent: int, source: str
) -> Tuple[Node, int]:
    node = Node()
    node.line = tokens[index][2] if index < len(tokens) else 0
    while index < len(tokens):
        current_indent, content, line = tokens[index]
        if current_indent < indent:
            break
        if current_indent > indent:
            raise ValidationError(
                f"unexpected indentation (expected {indent} spaces, got {current_indent})",
                line=line,
                source=source,
            )
        if content.startswith("- "):
            break
        key, sep, rest = content.partition(":")
        if not sep:
            raise ValidationError(
                f"expected 'key: value', got {content!r}", line=line, source=source
            )
        key = key.strip().strip("\"'")
        rest = rest.strip()
        index += 1
        if rest in ("|", ">"):
            value, index = _parse_block_scalar(tokens, index, indent, rest)
        elif rest == "":
            if index < len(tokens) and tokens[index][0] > indent:
                value, index = _parse_block(tokens, index, tokens[index][0], source)
            else:
                value = None
        else:
            value = _parse_scalar(rest, line, source)
        if key in node:
            raise ValidationError(
                f"duplicate key {key!r}", key=key, line=line, source=source
            )
        node[key] = value
        node.lines[key] = line
    return node, index


def _parse_block_scalar(
    tokens: List[Tuple[int, str, int]], index: int, indent: int, style: str
) -> Tuple[str, int]:
    lines: List[str] = []
    while index < len(tokens) and tokens[index][0] > indent:
        lines.append(tokens[index][1])
        index += 1
    joiner = "\n" if style == "|" else " "
    return joiner.join(lines), index


def _parse_sequence(
    tokens: List[Tuple[int, str, int]], index: int, indent: int, source: str
) -> Tuple[List[Any], int]:
    items: List[Any] = []
    while index < len(tokens):
        current_indent, content, line = tokens[index]
        if current_indent != indent or not content.startswith("- "):
            break
        rest = content[2:].strip()
        if rest == "":
            index += 1
            if index < len(tokens) and tokens[index][0] > indent:
                value, index = _parse_block(tokens, index, tokens[index][0], source)
            else:
                value = None
        elif ":" in rest and not rest[0] in "[{\"'":
            # "- id: extract" starts a mapping whose remaining keys are indented
            # to the column just after the dash.
            tokens[index] = (indent + 2, rest, line)
            value, index = _parse_mapping(tokens, index, indent + 2, source)
        else:
            value = _parse_scalar(rest, line, source)
            index += 1
        items.append(value)
    return items, index


def load_yaml(text: str, source: str = "<string>", loader: str = "builtin") -> Any:
    """Parse the supported YAML subset (or hand off to PyYAML)."""
    if loader == "pyyaml":  # pragma: no cover - optional dependency
        try:
            import yaml  # type: ignore
        except ImportError as exc:
            raise ValidationError(
                "loader='pyyaml' needs PyYAML installed: pip install PyYAML",
                source=source,
            ) from exc
        return yaml.safe_load(text)
    tokens = _tokenize(text)
    if not tokens:
        return Node()
    value, index = _parse_block(tokens, 0, tokens[0][0], source)
    if index != len(tokens):
        raise ValidationError(
            "trailing content that is not part of the document",
            line=tokens[index][2],
            source=source,
        )
    return value


# ------------------------------------------------------------------ validation

WORKFLOW_KEYS = {
    "name",
    "description",
    "deadline",
    "defaults",
    "schedule",
    "params",
    "tasks",
}
TASK_KEYS = {
    "id",
    "uses",
    "with",
    "depends_on",
    "retry",
    "timeout",
    "on_failure",
    "tags",
    "idempotency_key",
    "idempotent",
    "description",
}
DEFAULT_KEYS = {"retry", "timeout", "on_failure"}


def _check_keys(node: Mapping[str, Any], allowed: set, source: str, what: str) -> None:
    for key in node:
        if key in allowed:
            continue
        suggestion = difflib.get_close_matches(key, sorted(allowed), n=1, cutoff=0.6)
        hint = f"; did you mean {suggestion[0]!r}?" if suggestion else ""
        line = node.line_of(key) if isinstance(node, Node) else None
        raise ValidationError(
            f"unknown {what} key {key!r}{hint}. Allowed: {sorted(allowed)}",
            key=key,
            line=line,
            source=source,
        )


def validate_spec(spec: Any, source: str = "<string>") -> Node:
    """Structural validation of a parsed workflow document.

    Raises the first :class:`ValidationError` it finds, with key and line.
    """
    if not isinstance(spec, Mapping):
        raise ValidationError(
            f"a workflow file must be a mapping, got {type(spec).__name__}",
            source=source,
        )
    node = spec if isinstance(spec, Node) else Node(spec)
    _check_keys(node, WORKFLOW_KEYS, source, "workflow")
    if not node.get("name"):
        raise ValidationError("workflow needs a 'name'", key="name", source=source)
    tasks = node.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise ValidationError(
            "workflow needs a non-empty 'tasks' list",
            key="tasks",
            line=node.line_of("tasks"),
            source=source,
        )
    defaults = node.get("defaults") or Node()
    if defaults and not isinstance(defaults, Mapping):
        raise ValidationError(
            "'defaults' must be a mapping",
            key="defaults",
            line=node.line_of("defaults"),
            source=source,
        )
    if defaults:
        _check_keys(defaults, DEFAULT_KEYS, source, "defaults")

    seen: Dict[str, int] = {}
    for position, raw in enumerate(tasks):
        if not isinstance(raw, Mapping):
            raise ValidationError(
                f"task #{position + 1} must be a mapping, got {type(raw).__name__}",
                key="tasks",
                source=source,
            )
        task_node = raw if isinstance(raw, Node) else Node(raw)
        _check_keys(task_node, TASK_KEYS, source, "task")
        task_id = task_node.get("id")
        line = task_node.line_of("id")
        if not task_id or not isinstance(task_id, str):
            raise ValidationError(
                f"task #{position + 1} needs a string 'id'",
                key="id",
                line=task_node.line or line,
                source=source,
            )
        if task_id in seen:
            raise ValidationError(
                f"duplicate task id {task_id!r} (first defined on line {seen[task_id]})",
                key="id",
                line=line,
                source=source,
            )
        seen[task_id] = line or 0
        if not task_node.get("uses"):
            raise ValidationError(
                f"task {task_id!r} needs a 'uses' target "
                f"(python:module:function or connector:name)",
                key="uses",
                line=task_node.line or line,
                source=source,
            )
        depends = task_node.get("depends_on") or []
        if isinstance(depends, str):
            depends = [depends]
        if not isinstance(depends, list):
            raise ValidationError(
                f"task {task_id!r}: 'depends_on' must be a list",
                key="depends_on",
                line=task_node.line_of("depends_on"),
                source=source,
            )
        on_failure = task_node.get("on_failure")
        if on_failure is not None and str(on_failure) not in {
            policy.value for policy in OnFailure
        }:
            raise ValidationError(
                f"task {task_id!r}: on_failure must be one of "
                f"{[p.value for p in OnFailure]}, got {on_failure!r}",
                key="on_failure",
                line=task_node.line_of("on_failure"),
                source=source,
            )
        with_block = task_node.get("with")
        if with_block is not None and not isinstance(with_block, Mapping):
            raise ValidationError(
                f"task {task_id!r}: 'with' must be a mapping of call arguments",
                key="with",
                line=task_node.line_of("with"),
                source=source,
            )

    # Dependency targets are checked here rather than in Dag.validate so the
    # error can point at the line that names the missing task.
    for raw in tasks:
        task_node = raw if isinstance(raw, Node) else Node(raw)
        depends = task_node.get("depends_on") or []
        if isinstance(depends, str):
            depends = [depends]
        for dep in depends:
            if dep not in seen:
                raise ValidationError(
                    f"task {task_node.get('id')!r} depends on {dep!r}, "
                    f"which is not defined in this file",
                    key="depends_on",
                    line=task_node.line_of("depends_on"),
                    source=source,
                )
    return node


# --------------------------------------------------------------------- building


def _resolve_python(target: str, source: str, line: Optional[int]) -> Callable[..., Any]:
    """Resolve ``python:package.module:function``."""
    body = target.split(":", 1)[1]
    module_name, _, attribute = body.rpartition(":")
    if not module_name or not attribute:
        raise ValidationError(
            f"python target {target!r} must look like python:module:function",
            key="uses",
            line=line,
            source=source,
        )
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ValidationError(
            f"cannot import module {module_name!r} for {target!r}: {exc}",
            key="uses",
            line=line,
            source=source,
        ) from None
    try:
        resolved = getattr(module, attribute)
    except AttributeError:
        available = [n for n in dir(module) if not n.startswith("_")][:10]
        raise ValidationError(
            f"{module_name!r} has no attribute {attribute!r}. Some names it does "
            f"have: {available}",
            key="uses",
            line=line,
            source=source,
        ) from None
    if not callable(resolved):
        raise ValidationError(
            f"python target {target!r} resolved to a "
            f"{type(resolved).__name__}, which is not callable",
            key="uses",
            line=line,
            source=source,
        )
    return resolved


def build_workflow(
    spec: Mapping[str, Any],
    *,
    source: str = "<string>",
    connectors: Optional[Mapping[str, Any]] = None,
    resolve: bool = True,
) -> Workflow:
    """Turn a validated spec into a :class:`~flowforge.workflow.Workflow`."""
    node = validate_spec(spec, source)
    defaults = node.get("defaults") or {}
    workflow = Workflow(
        str(node["name"]),
        description=str(node.get("description") or ""),
        deadline=node.get("deadline"),
    )
    registry = dict(connectors or {})
    for raw in node["tasks"]:
        task_node = raw if isinstance(raw, Node) else Node(raw)
        task_id = str(task_node["id"])
        line = task_node.line_of("uses")
        uses = str(task_node["uses"])
        depends = task_node.get("depends_on") or []
        if isinstance(depends, str):
            depends = [depends]
        retry_spec = task_node.get("retry", defaults.get("retry"))
        common: Dict[str, Any] = {
            "depends_on": tuple(depends),
            "retry": policy_from_spec(retry_spec),
            "timeout": task_node.get("timeout", defaults.get("timeout")),
            "on_failure": OnFailure(
                str(task_node.get("on_failure", defaults.get("on_failure", "fail")))
            ),
            "tags": frozenset(task_node.get("tags") or ()),
            "description": str(task_node.get("description") or ""),
        }
        arguments = dict(task_node.get("with") or {})
        key_spec = task_node.get("idempotency_key")

        if uses.startswith("connector:"):
            name = uses.split(":", 1)[1]
            if name not in registry:
                if not resolve:
                    workflow.add(_placeholder(task_id, uses, common))
                    continue
                raise ValidationError(
                    f"task {task_id!r} uses connector {name!r}, which was not "
                    f"supplied. Known connectors: {sorted(registry) or 'none'}",
                    key="uses",
                    line=line,
                    source=source,
                )
            connector = registry[name]
            workflow.add(
                connector.as_task(
                    task_id,
                    idempotency_key=key_spec,
                    **common,
                    **arguments,
                )
            )
            continue

        if uses.startswith("python:"):
            if not resolve:
                workflow.add(_placeholder(task_id, uses, common))
                continue
            function = _resolve_python(uses, source, line)
            if arguments:
                base = function

                def function(ctx: TaskContext, _base: Any = base, _kw: Dict[str, Any] = arguments) -> Any:
                    return _base(ctx, **_kw)

            workflow.add(
                Task(
                    id=task_id,
                    fn=function,
                    idempotency_key=_key_from_spec(
                        key_spec, task_id, source, task_node.line_of("idempotency_key")
                    ),
                    idempotent=bool(task_node.get("idempotent", False)),
                    **common,
                )
            )
            continue

        raise ValidationError(
            f"task {task_id!r}: 'uses' must start with 'python:' or 'connector:', "
            f"got {uses!r}",
            key="uses",
            line=line,
            source=source,
        )
    return workflow


def _key_from_spec(
    spec: Any, task_id: str, source: str, line: Optional[int]
) -> Any:
    """Turn a YAML ``idempotency_key`` value into a key or a key function.

    A plain string is used literally -- which means "run this at most once,
    ever, for that key", so it almost always wants a parameter in it. The
    ``{param:name}`` placeholder is substituted from the run parameters:

        idempotency_key: "notify:{param:run_date}"

    ``auto`` is only meaningful for connector steps, where the call arguments
    are known and can be hashed. On a Python step there is nothing to hash, so
    it is rejected rather than silently turned into a constant.
    """
    if spec is None:
        return None
    if not isinstance(spec, str):
        raise ValidationError(
            f"task {task_id!r}: idempotency_key must be a string",
            key="idempotency_key",
            line=line,
            source=source,
        )
    if spec == "auto":
        raise ValidationError(
            f"task {task_id!r}: idempotency_key 'auto' needs a connector step "
            f"(its call arguments are what gets hashed). For a python step, give "
            f"an explicit key such as \"{task_id}:{{param:run_date}}\".",
            key="idempotency_key",
            line=line,
            source=source,
        )
    if "{param:" not in spec:
        return spec

    def _render(ctx: TaskContext, _template: str = spec) -> str:
        out = _template
        for name, value in sorted(ctx.params.items()):
            out = out.replace(f"{{param:{name}}}", str(value))
        if "{param:" in out:
            missing = out.split("{param:", 1)[1].split("}", 1)[0]
            raise ValidationError(
                f"idempotency key template needs run parameter {missing!r}",
                key="idempotency_key",
            )
        return out

    return _render


def _placeholder(task_id: str, uses: str, common: Dict[str, Any]) -> Task:
    """A task that validates structurally but refuses to run."""

    def _unresolved(ctx: TaskContext) -> Any:
        raise ValidationError(
            f"task {task_id!r} was loaded with resolve=False and cannot run "
            f"({uses})",
            key="uses",
        )

    return Task(id=task_id, fn=_unresolved, **common)


def load_workflow(
    text: str,
    *,
    source: str = "<string>",
    connectors: Optional[Mapping[str, Any]] = None,
    resolve: bool = True,
    loader: str = "builtin",
) -> Workflow:
    """Parse and build a workflow from YAML text."""
    spec = load_yaml(text, source=source, loader=loader)
    return build_workflow(spec, source=source, connectors=connectors, resolve=resolve)


def load_workflow_file(
    path: str,
    *,
    connectors: Optional[Mapping[str, Any]] = None,
    resolve: bool = True,
    loader: str = "builtin",
) -> Workflow:
    """Parse and build a workflow from a file, reporting errors against it."""
    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    return load_workflow(
        text,
        source=os.path.basename(path),
        connectors=connectors,
        resolve=resolve,
        loader=loader,
    )


def schedule_of(spec: Mapping[str, Any]) -> Optional[Schedule]:
    """Build the schedule declared in a workflow document, if any."""
    block = spec.get("schedule")
    if block is None:
        return None
    return schedule_from_spec(block)
