"""Contains graphs used for testing."""

from bout_runners.runner.run_graph import RunGraph


def simple_graph() -> RunGraph:
    """
    Return a simple graph.

    Returns
    -------
    graph : RunGraph
        A simple graph
    """
    graph = RunGraph()

    graph.add_edge("0", "1")
    graph.add_edge("0", "2")

    graph.add_edge("1", "3")
    graph.add_edge("1", "4")

    return graph


def complex_graph() -> RunGraph:
    """
    Return a complex graph.

    Returns
    -------
    graph : RunGraph
        A simple graph
    """
    graph = RunGraph()

    graph.add_edge("0", "2")
    graph.add_edge("1", "2")

    graph.add_edge("2", "3")
    graph.add_edge("2", "5")

    graph.add_edge("2", "6")
    graph.add_edge("2", "7")

    graph.add_edge("4", "9")
    graph.add_edge("6", "9")
    graph.add_edge("7", "9")

    graph.add_edge("9", "10")

    graph.add_edge("4", "8")
    graph.add_edge("6", "8")

    graph.add_edge("8", "10")

    graph.add_edge("12", "11")
    graph.add_edge("11", "4")

    return graph


def another_complex_graph() -> RunGraph:
    """
    Return another complex graph.

    Returns
    -------
    graph : RunGraph
        A simple graph
    """
    graph = RunGraph()

    graph.add_edge("0", "2")
    graph.add_edge("0", "3")
    graph.add_edge("0", "4")

    graph.add_edge("1", "5")
    graph.add_edge("1", "6")

    graph.add_edge("3", "7")
    graph.add_edge("3", "8")

    graph.add_edge("4", "9")

    graph.add_edge("5", "9")

    graph.add_edge("8", "10")
    graph.add_edge("9", "10")

    return graph
