"""Contains unittests for the run graph."""

import pytest
from bout_runners.runner.run_graph import RunGraph


def test_add_node() -> None:
    """Test ability to write and rewrite a node."""
    run_graph = RunGraph()
    run_graph.add_node("test", args=("pass", 42))
    assert len(run_graph.nodes) == 1
    assert run_graph.nodes["test"] == {
        "function": None,
        "args": ("pass", 42),
        "kwargs": None,
    }
    run_graph.add_node("test")
    assert len(run_graph.nodes) == 1
    assert run_graph.nodes["test"] == {"function": None, "args": None, "kwargs": None}


def test_add_edge() -> None:
    """Test ability to add edges, and the ability to detect if a graph is cyclic."""
    run_graph = RunGraph()
    run_graph.add_node("1")
    run_graph.add_node("2")

    run_graph.add_edge("1", "2")

    with pytest.raises(ValueError):
        run_graph.add_edge("2", "1")

    expected = {"1", "2"}

    assert expected == set(run_graph.nodes)


def test_add_waiting_for() -> None:
    """Test the ability to let a node wait for other nodes."""
    run_graph = RunGraph()
    run_graph.add_node("1")
    run_graph.add_node("2")
    run_graph.add_node("3")

    run_graph.add_waiting_for("2", "1")
    run_graph.add_waiting_for("3", ("2", "1"))

    expected = ("1", "2", "3")

    assert expected == run_graph.get_waiting_for_tuple("1")


def test_get_waiting_for_tuple(make_graph) -> None:
    """
    Test the waiting for tuple functionality.

    Parameters
    ----------
    make_graph : RunGraph
        A simple graph
    """
    run_graph = make_graph
    expected = ("2", "3", "4", "5")
    assert expected == run_graph.get_waiting_for_tuple("2")


def test_remove_node_with_dependencies(make_graph) -> None:
    """
    Test the remove node functionality.

    Parameters
    ----------
    make_graph : RunGraph
        A simple graph
    """
    run_graph = make_graph
    expected = {"0", "1"}
    run_graph.remove_node_with_dependencies("2")
    assert expected == set(run_graph.nodes)


def test_pick_root_nodes(make_graph) -> None:
    """
    Test the pick root nodes functionality.

    Parameters
    ----------
    make_graph : RunGraph
        A simple graph
    """
    run_graph = make_graph
    nodes = run_graph.pick_root_nodes()
    expected_after_1st_pick = ({"args": None, "function": None, "kwargs": None},)
    assert expected_after_1st_pick == nodes

    nodes = run_graph.pick_root_nodes()
    expected_after_2nd_pick = (
        {"args": None, "function": None, "kwargs": None},
        {"args": None, "function": None, "kwargs": None},
    )
    assert expected_after_2nd_pick == nodes
    expected_remaining_nodes = {"3", "4", "5"}
    assert expected_remaining_nodes == set(run_graph.nodes)


def test_get_dot_string() -> None:
    """Test the ability to get the dot string."""
    run_graph = RunGraph()
    run_graph.add_node("42")
    expected = "strict digraph  {\n42 [args=None, function=None, kwargs=None];\n}\n"
    assert expected == run_graph.get_dot_string()
