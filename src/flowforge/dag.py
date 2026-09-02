"""Directed acyclic graph of task ids.

This module knows nothing about running anything. It answers structural
questions: what order, what can go in parallel, what is unreachable, and --
when the graph is broken -- *which specific tasks* form the cycle.

Every ordering is deterministic. Ties are broken by sorting task ids, so the
same workflow always produces the same plan and diffs of a rendered graph are
meaningful.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from .errors import CycleError, DuplicateTaskError, UnknownTaskError

__all__ = ["Dag"]


class Dag:
    """A graph of node ids with ``depends_on`` edges.

    An edge ``a -> b`` means "b depends on a", i.e. a must finish first.
    """

    def __init__(self) -> None:
        self._deps: Dict[str, Set[str]] = {}
        self._order: List[str] = []  # declaration order, used for stable output

    # ------------------------------------------------------------------ build

    def add_node(self, node_id: str, depends_on: Iterable[str] = ()) -> "Dag":
        """Add a node. Raises :class:`DuplicateTaskError` if the id is taken."""
        if node_id in self._deps:
            raise DuplicateTaskError(f"duplicate task id {node_id!r}")
        if not node_id or not isinstance(node_id, str):
            raise ValueError(f"task id must be a non-empty string, got {node_id!r}")
        self._deps[node_id] = set(depends_on)
        self._order.append(node_id)
        return self

    def add_edge(self, upstream: str, downstream: str) -> "Dag":
        """Declare that ``downstream`` runs after ``upstream``."""
        if downstream not in self._deps:
            raise UnknownTaskError(downstream)
        self._deps[downstream].add(upstream)
        return self

    # ------------------------------------------------------------------ query

    def __contains__(self, node_id: object) -> bool:
        return node_id in self._deps

    def __len__(self) -> int:
        return len(self._deps)

    def __iter__(self):
        return iter(self.nodes)

    @property
    def nodes(self) -> List[str]:
        """Nodes in declaration order."""
        return list(self._order)

    def dependencies(self, node_id: str) -> List[str]:
        """Direct upstream nodes, sorted."""
        self._require(node_id)
        return sorted(self._deps[node_id])

    def dependents(self, node_id: str) -> List[str]:
        """Direct downstream nodes, sorted."""
        self._require(node_id)
        return sorted(n for n, deps in self._deps.items() if node_id in deps)

    def edges(self) -> List[Tuple[str, str]]:
        """All ``(upstream, downstream)`` pairs, sorted."""
        out = [(dep, node) for node, deps in self._deps.items() for dep in deps]
        return sorted(out)

    def roots(self) -> List[str]:
        """Nodes with no dependencies."""
        return sorted(n for n, deps in self._deps.items() if not deps)

    def leaves(self) -> List[str]:
        """Nodes nothing depends on."""
        depended_on = {d for deps in self._deps.values() for d in deps}
        return sorted(n for n in self._deps if n not in depended_on)

    def ancestors(self, node_id: str) -> Set[str]:
        """Every node that must run before ``node_id``, transitively."""
        self._require(node_id)
        seen: Set[str] = set()
        stack = list(self._deps[node_id])
        while stack:
            cur = stack.pop()
            if cur in seen or cur not in self._deps:
                continue
            seen.add(cur)
            stack.extend(self._deps[cur])
        return seen

    def descendants(self, node_id: str) -> Set[str]:
        """Every node blocked by ``node_id``, transitively.

        This is what the executor uses to mark a failure's blast radius.
        """
        self._require(node_id)
        children: Dict[str, List[str]] = {n: [] for n in self._deps}
        for node, deps in self._deps.items():
            for dep in deps:
                if dep in children:
                    children[dep].append(node)
        seen: Set[str] = set()
        stack = list(children[node_id])
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(children[cur])
        return seen

    # ------------------------------------------------------------- validation

    def find_cycle(self) -> Optional[List[str]]:
        """Return a cycle as a node path, or ``None``.

        The returned path starts and ends on the same node, e.g.
        ``["build", "test", "deploy", "build"]``. Iterative DFS with an explicit
        stack so a 10k-node graph does not blow the recursion limit.
        """
        WHITE, GREY, BLACK = 0, 1, 2
        colour: Dict[str, int] = {n: WHITE for n in self._deps}
        parent: Dict[str, Optional[str]] = {n: None for n in self._deps}

        for start in sorted(self._deps):
            if colour[start] != WHITE:
                continue
            stack: List[Tuple[str, List[str]]] = [
                (start, sorted(d for d in self._deps[start] if d in self._deps))
            ]
            colour[start] = GREY
            while stack:
                node, pending = stack[-1]
                if not pending:
                    colour[node] = BLACK
                    stack.pop()
                    continue
                nxt = pending.pop(0)
                if colour[nxt] == GREY:
                    # Back edge: `node` depends on `nxt`, and `nxt` is still on
                    # the DFS stack. Walk parent links from `node` up to `nxt`.
                    # Parent links point towards the DFS root, and we descend
                    # along "depends on" edges, so that walk is already in
                    # execution order.
                    path = [node]
                    cur = node
                    while cur != nxt:
                        cur = parent[cur]  # type: ignore[assignment]
                        path.append(cur)
                    # Rotate so the cycle starts (and ends) at the node that
                    # closed it, which reads the way a person would draw it.
                    return [path[-1]] + path[:-1] + [path[-1]]
                if colour[nxt] == WHITE:
                    colour[nxt] = GREY
                    parent[nxt] = node
                    stack.append(
                        (nxt, sorted(d for d in self._deps[nxt] if d in self._deps))
                    )
        return None

    def missing_dependencies(self) -> List[Tuple[str, str]]:
        """``(node, missing_dep)`` pairs for dependencies that do not exist."""
        out = []
        for node in self._order:
            for dep in sorted(self._deps[node]):
                if dep not in self._deps:
                    out.append((node, dep))
        return out

    def validate(self) -> None:
        """Raise if the graph is unusable.

        Order matters: a dangling dependency is reported before a cycle, because
        a typo in a dependency name is the more common mistake and reporting a
        cycle first would send you looking in the wrong place.
        """
        missing = self.missing_dependencies()
        if missing:
            node, dep = missing[0]
            raise UnknownTaskError(dep, referenced_by=node)
        cycle = self.find_cycle()
        if cycle:
            raise CycleError(cycle)

    def isolated_nodes(self) -> List[str]:
        """Nodes with no dependencies *and* no dependents.

        Usually a typo in a ``depends_on`` somewhere else, or a task somebody
        forgot to wire in after a refactor. It still runs; it is just suspicious
        enough to warn about.
        """
        depended_on = {d for deps in self._deps.values() for d in deps}
        return sorted(
            n for n in self._deps if not self._deps[n] and n not in depended_on
        )

    def unreachable_from(self, entries: Sequence[str]) -> List[str]:
        """Nodes that no path from ``entries`` reaches.

        If you trigger a workflow at a specific task, these will never run.
        """
        for entry in entries:
            self._require(entry)
        reachable: Set[str] = set()
        stack = list(entries)
        while stack:
            cur = stack.pop()
            if cur in reachable:
                continue
            reachable.add(cur)
            stack.extend(self.dependents(cur))
        return sorted(n for n in self._deps if n not in reachable)

    # -------------------------------------------------------------- ordering

    def topological_order(self) -> List[str]:
        """Deterministic topological sort (Kahn, ready set drained in id order)."""
        self.validate()
        indegree = {n: len(self._deps[n]) for n in self._deps}
        ready = sorted(n for n, d in indegree.items() if d == 0)
        out: List[str] = []
        while ready:
            node = ready.pop(0)
            out.append(node)
            for child in self.dependents(node):
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
                    ready.sort()
        return out

    def levels(self) -> List[List[str]]:
        """Group nodes into waves that may run in parallel.

        Level *i* contains every node whose longest path from a root is *i*.
        Everything in a level is mutually independent, so the executor can hand
        a whole level to the thread pool at once.
        """
        self.validate()
        depth: Dict[str, int] = {}
        for node in self.topological_order():
            deps = self._deps[node]
            depth[node] = 0 if not deps else 1 + max(depth[d] for d in deps)
        levels: List[List[str]] = [[] for _ in range(max(depth.values(), default=-1) + 1)]
        for node, d in depth.items():
            levels[d].append(node)
        return [sorted(level) for level in levels]

    def critical_path(self) -> List[str]:
        """Longest dependency chain: the floor on wall-clock time.

        No matter how many workers you add, the run cannot finish faster than
        this chain runs end to end.
        """
        self.validate()
        best: Dict[str, List[str]] = {}
        for node in self.topological_order():
            deps = self._deps[node]
            if not deps:
                best[node] = [node]
            else:
                longest = max(
                    (best[d] for d in sorted(deps)), key=lambda p: (len(p), p)
                )
                best[node] = longest + [node]
        if not best:
            return []
        return max(best.values(), key=lambda p: (len(p), p))

    # ---------------------------------------------------------------- render

    def render(self, marks: Optional[Dict[str, str]] = None) -> str:
        """ASCII rendering, one level per block, dependencies annotated.

        ``marks`` maps a node id to a short status glyph shown next to it.
        """
        marks = marks or {}
        lines: List[str] = []
        for i, level in enumerate(self.levels()):
            lines.append(f"level {i}")
            for node in level:
                deps = self.dependencies(node)
                mark = marks.get(node, "")
                label = f"{node} {mark}".strip()
                arrow = "  o " if not deps else "  +-"
                suffix = "" if not deps else f"  <- {', '.join(deps)}"
                lines.append(f"{arrow} {label}{suffix}")
        return "\n".join(lines)

    # ---------------------------------------------------------------- private

    def _require(self, node_id: str) -> None:
        if node_id not in self._deps:
            raise UnknownTaskError(node_id)
