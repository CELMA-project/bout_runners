"""Contains unittests for the run graph."""

import pytest
from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.submitter.local_submitter import LocalSubmitter


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


def test___next__(make_graph) -> None:
    """
    Test the next functionality.

    Parameters
    ----------
    make_graph : RunGraph
        A simple graph
    """
    run_graph = make_graph
    nodes = next(run_graph)

    assert nodes["0"]["function"] is None
    assert nodes["0"]["args"] is None
    assert nodes["0"]["kwargs"] is None
    assert nodes["0"]["status"] == "traversed"
    assert isinstance(nodes["0"]["submitter"], LocalSubmitter)

    nodes = next(run_graph)
    for number_str in ["1", "2"]:
        assert nodes[number_str]["function"] is None
        assert nodes[number_str]["args"] is None
        assert nodes[number_str]["kwargs"] is None
        assert nodes[number_str]["status"] == "traversed"
        assert isinstance(nodes[number_str]["submitter"], LocalSubmitter)

    nodes_with_ready_status = ("3", "4", "5")
    for node_name in run_graph.nodes:
        if node_name in nodes_with_ready_status:
            assert run_graph[node_name]["status"] == "ready"
        else:
            assert run_graph[node_name]["status"] == "traversed"


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
    _ = next(run_graph)
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
