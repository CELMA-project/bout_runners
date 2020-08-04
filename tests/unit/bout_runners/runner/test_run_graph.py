"""Contains unittests for the run graph."""

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
