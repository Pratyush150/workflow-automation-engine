"""Graph structure: ordering, levels, and cycles that name themselves."""

from __future__ import annotations

import pytest

from flowforge.dag import Dag
from flowforge.errors import CycleError, DuplicateTaskError, UnknownTaskError


def diamond() -> Dag:
    """a -> (b, c) -> d."""
    graph = Dag()
    graph.add_node("a")
    graph.add_node("b", ["a"])
    graph.add_node("c", ["a"])
    graph.add_node("d", ["b", "c"])
    return graph


def test_topological_order_is_deterministic():
    graph = diamond()
    assert graph.topological_order() == ["a", "b", "c", "d"]
    assert graph.topological_order() == graph.topological_order()


def test_diamond_levels_group_parallel_work():
    assert diamond().levels() == [["a"], ["b", "c"], ["d"]]


def test_diamond_relationships():
    graph = diamond()
    assert graph.roots() == ["a"]
    assert graph.leaves() == ["d"]
    assert graph.dependencies("d") == ["b", "c"]
    assert graph.dependents("a") == ["b", "c"]
    assert graph.descendants("a") == {"b", "c", "d"}
    assert graph.ancestors("d") == {"a", "b", "c"}
    assert graph.critical_path() == ["a", "c", "d"]
    assert len(graph) == 4
    assert "b" in graph


def test_three_node_cycle_reports_the_actual_cycle():
    graph = Dag()
    graph.add_node("build", ["deploy"])
    graph.add_node("test", ["build"])
    graph.add_node("deploy", ["test"])

    cycle = graph.find_cycle()
    assert cycle is not None
    # A real path: same node at both ends, every hop a real edge.
    assert cycle[0] == cycle[-1]
    assert len(cycle) == 4
    assert set(cycle) == {"build", "test", "deploy"}
    for upstream, downstream in zip(cycle, cycle[1:]):
        assert upstream in graph.dependencies(downstream)

    with pytest.raises(CycleError) as excinfo:
        graph.validate()
    message = str(excinfo.value)
    assert " -> ".join(cycle) in message
    assert excinfo.value.cycle == cycle


def test_self_dependency_is_a_cycle():
    graph = Dag()
    graph.add_node("loop", ["loop"])
    assert graph.find_cycle() == ["loop", "loop"]


def test_cycle_is_found_even_when_the_graph_has_a_clean_part():
    graph = Dag()
    graph.add_node("clean")
    graph.add_node("x", ["y"])
    graph.add_node("y", ["x"])
    cycle = graph.find_cycle()
    assert cycle is not None
    assert "clean" not in cycle


def test_acyclic_graph_reports_no_cycle():
    assert diamond().find_cycle() is None


def test_missing_dependency_is_reported_before_a_cycle():
    graph = Dag()
    graph.add_node("a", ["typo"])
    with pytest.raises(UnknownTaskError) as excinfo:
        graph.validate()
    assert "typo" in str(excinfo.value)
    assert "'a'" in str(excinfo.value)


def test_duplicate_node_id_rejected():
    graph = Dag()
    graph.add_node("a")
    with pytest.raises(DuplicateTaskError):
        graph.add_node("a")


def test_isolated_and_unreachable_tasks_are_detected():
    graph = diamond()
    graph.add_node("orphan")
    assert graph.isolated_nodes() == ["orphan"]
    assert graph.unreachable_from(["a"]) == ["orphan"]
    # Starting at b, only b and d run: a and c are upstream or off the path.
    assert graph.unreachable_from(["b"]) == ["a", "c", "orphan"]


def test_render_shows_levels_and_dependencies():
    text = diamond().render({"d": "[slow]"})
    assert "level 0" in text
    assert "d [slow]  <- b, c" in text
