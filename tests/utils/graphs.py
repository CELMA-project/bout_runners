"""Contains graphs used for testing."""

import networkx as nx


def simple_graph() -> nx.DiGraph:
    """
    Return a simple graph.

    Returns
    -------
    graph : nx.Digraph
        A simple graph
    """
    graph = nx.DiGraph()

    for i in range(5):
        graph.add_node(i, status="ready")

    graph.add_edge(0, 1)
    graph.add_edge(0, 2)

    graph.add_edge(1, 3)
    graph.add_edge(1, 4)

    return graph


def complex_graph() -> nx.DiGraph:
    """
    Return a complex graph.

    Returns
    -------
    graph : nx.Digraph
        A simple graph
    """
    graph = nx.DiGraph()

    for i in range(13):
        graph.add_node(i, status="ready")

    graph.add_edge(0, 2)
    graph.add_edge(1, 2)

    graph.add_edge(2, 3)
    graph.add_edge(2, 5)

    graph.add_edge(2, 6)
    graph.add_edge(2, 7)

    graph.add_edge(4, 9)
    graph.add_edge(6, 9)
    graph.add_edge(7, 9)

    graph.add_edge(9, 10)

    graph.add_edge(4, 8)
    graph.add_edge(6, 8)

    graph.add_edge(8, 10)

    graph.add_edge(12, 11)
    graph.add_edge(11, 4)

    return graph


def another_complex_graph() -> nx.DiGraph:
    """
    Return another complex graph.

    Returns
    -------
    graph : nx.Digraph
        A simple graph
    """
    graph = nx.DiGraph()

    for i in range(11):
        graph.add_node(i, status="ready")

    graph.add_edge(0, 2)
    graph.add_edge(0, 3)
    graph.add_edge(0, 4)

    graph.add_edge(1, 5)
    graph.add_edge(1, 6)

    graph.add_edge(3, 7)
    graph.add_edge(3, 8)

    graph.add_edge(4, 9)

    graph.add_edge(5, 9)

    graph.add_edge(8, 10)
    graph.add_edge(9, 10)

    return graph
