"""Contains unittests for the run graph."""


from typing import Tuple

import pytest

from bout_runners.parameters.bout_run_setup import BoutRunSetup
from bout_runners.runner.run_graph import RunGraph
from bout_runners.submitter.local_submitter import LocalSubmitter
from tests.utils.graphs import another_complex_graph, complex_graph, simple_graph


def test_add_bout_run_node(get_bout_run_setup) -> None:
    """
    Test ability to write and rewrite a BoutRunSetup node.

    Parameters
    ----------
    get_bout_run_setup : function
        Function which returns the BoutRunSetup object based on the conduction directory
    """
    run_graph = RunGraph()
    bout_run_setup = get_bout_run_setup("test_run_graph")
    run_graph.add_bout_run_node("test", bout_run_setup)
    assert len(run_graph.nodes) == 1
    assert isinstance(run_graph["test"]["bout_run_setup"], BoutRunSetup)

    with pytest.raises(ValueError):
        run_graph.add_function_node("test")
        assert len(run_graph.nodes) == 1


def test_add_function_node() -> None:
    """Test ability to write and rewrite a function node."""
    run_graph = RunGraph()
    run_graph.add_function_node(
        "test", function_dict={"function": None, "args": ("pass", 42), "kwargs": None}
    )
    assert len(run_graph.nodes) == 1
    assert run_graph["test"]["function"] is None
    assert run_graph["test"]["args"] == ("pass", 42)
    assert run_graph["test"]["kwargs"] is None
    assert isinstance(run_graph["test"]["submitter"], LocalSubmitter)

    with pytest.raises(ValueError):
        run_graph.add_function_node("test")
        assert len(run_graph.nodes) == 1


def test_add_edge() -> None:
    """Test ability to add edges, and the ability to detect if a graph is cyclic."""
    run_graph = RunGraph()
    run_graph.add_function_node("1")
    run_graph.add_function_node("2")

    run_graph.add_edge("1", "2")

    with pytest.raises(ValueError):
        run_graph.add_edge("2", "1")

    expected = {"1", "2"}

    assert expected == set(run_graph.nodes)


def test_add_waiting_for() -> None:
    """Test the ability to let a node wait for other nodes."""
    run_graph = RunGraph()
    run_graph.add_function_node("1")
    run_graph.add_function_node("2")
    run_graph.add_function_node("3")

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


def test_change_status_node_and_dependencies(make_graph) -> None:
    """
    Test the remove node functionality.

    Parameters
    ----------
    make_graph : RunGraph
        A simple graph
    """
    run_graph = make_graph
    status = "foo"

    run_graph.change_status_node_and_dependencies("2", status=status)
    nodes_with_ready_status = ("0", "1")
    for node_name in run_graph.nodes:
        if node_name in nodes_with_ready_status:
            assert run_graph[node_name]["status"] == "ready"
        else:
            assert run_graph[node_name]["status"] == status


def test_reset(make_graph) -> None:
    """
    Test the remove node functionality.

    Parameters
    ----------
    make_graph : RunGraph
        A simple graph
    """
    run_graph = make_graph

    length = len(run_graph)
    run_graph.change_status_node_and_dependencies("2")
    # Only "0" and "1" will have status ready
    assert len(run_graph) == 2

    run_graph.reset()
    assert len(run_graph) == length


@pytest.mark.timeout(30)
def test___iter__(make_graph) -> None:
    """
    Test the __iter__ functionality.

    Parameters
    ----------
    make_graph : RunGraph
        A simple graph
    """
    run_graph = make_graph
    order = (("0",), ("1", "2"), ("3",), ("4", "5"))
    for number, nodes in enumerate(run_graph):
        assert order[number] == nodes
    for number, nodes in enumerate(run_graph):
        assert order[number] == nodes


def test___next__(make_graph) -> None:
    """
    Test the next functionality.

    Parameters
    ----------
    make_graph : RunGraph
        A simple graph
    """
    run_graph = make_graph
    first_order_nodes = next(run_graph)
    assert first_order_nodes == ("0",)

    second_order_nodes = next(run_graph)
    assert second_order_nodes == ("1", "2")

    third_order_nodes = next(run_graph)
    assert third_order_nodes == ("3",)

    fourth_order_nodes = next(run_graph)
    assert fourth_order_nodes == ("4", "5")

    with pytest.raises(StopIteration):
        _ = next(run_graph)

    for node_name in [
        *first_order_nodes,
        *second_order_nodes,
        *third_order_nodes,
        *fourth_order_nodes,
    ]:
        assert run_graph[node_name]["function"] is None
        assert run_graph[node_name]["args"] is None
        assert run_graph[node_name]["kwargs"] is None
        assert run_graph[node_name]["status"] == "ready"
        assert isinstance(run_graph[node_name]["submitter"], LocalSubmitter)


def test___len__(make_graph) -> None:
    """
    Test the length functionality.

    Parameters
    ----------
    make_graph : RunGraph
        A simple graph
    """
    run_graph = make_graph
    assert len(run_graph) == len(run_graph.nodes)

    # In this special case only one node is traversed
    node = next(run_graph)
    run_graph[node[0]]["status"] = "submitted"
    assert len(run_graph) == len(run_graph.nodes) - 1


def test_get_dot_string() -> None:
    """Test the ability to get the dot string."""
    run_graph = RunGraph()
    run_graph.add_function_node("42")
    hex_id_submitter = hex(id(run_graph["42"]["submitter"]))
    expected = (
        "strict digraph  "
        "{\n42 [args=None, function=None, kwargs=None, path=None, status=ready, "
        "submitter=<bout_runners.submitter.local_submitter.LocalSubmitter object at "
        f"{hex_id_submitter}"
        ">];\n}\n"
    )
    assert expected == run_graph.get_dot_string()


@pytest.mark.parametrize(
    "graph, reverse, expected",
    [
        (simple_graph(), False, (("0",), ("1", "2"), ("3", "4"))),
        (
            complex_graph(),
            False,
            (
                ("0", "1", "12"),
                ("2", "11"),
                ("3", "5", "6", "7", "4"),
                ("9", "8"),
                ("10",),
            ),
        ),
        (
            another_complex_graph(),
            False,
            (("0", "1"), ("2", "3", "4", "5", "6"), ("7", "8", "9"), ("10",)),
        ),
        (simple_graph(), True, (("2", "3", "4"), ("1",), ("0",))),
        (
            complex_graph(),
            True,
            (
                ("3", "5", "10"),
                ("9", "8"),
                ("6", "7", "4"),
                ("2", "11"),
                ("0", "1", "12"),
            ),
        ),
        (
            another_complex_graph(),
            True,
            (("2", "6", "7", "10"), ("8", "9"), ("3", "4", "5"), ("0", "1")),
        ),
    ],
)
def test_get_nodes_orders(
    graph: RunGraph, reverse: bool, expected: Tuple[Tuple[int, ...], ...]
) -> None:
    """
    Test that get_node_orders works as expected.

    Parameters
    ----------
    graph : nx.DiGraph
        The graph to search
    reverse : bool
        Whether or not to reverse search
    expected : tuple of int
        The expected result
    """
    result = graph.get_node_orders(reverse)
    assert result == expected
