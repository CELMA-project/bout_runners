import networkx as nx


def simple_graph() -> nx.DiGraph:
    """
    Return a simple graph.

    Returns
    -------
    g : nx.Digraph
        A simple graph
    """
    g = nx.DiGraph()

    for i in range(5):
        g.add_node(i, status="ready")

    g.add_edge(0, 1)
    g.add_edge(0, 2)

    g.add_edge(1, 3)
    g.add_edge(1, 4)

    return g


def complex_graph() -> nx.DiGraph:
    """
    Return a complex graph.

    Returns
    -------
    g : nx.Digraph
        A simple graph
    """
    g = nx.DiGraph()

    for i in range(13):
        g.add_node(i, status="ready")

    g.add_edge(0, 2)
    g.add_edge(1, 2)

    g.add_edge(2, 3)
    g.add_edge(2, 5)

    g.add_edge(2, 6)
    g.add_edge(2, 7)

    g.add_edge(4, 9)
    g.add_edge(6, 9)
    g.add_edge(7, 9)

    g.add_edge(9, 10)

    g.add_edge(4, 8)
    g.add_edge(6, 8)

    g.add_edge(8, 10)

    g.add_edge(12, 11)
    g.add_edge(11, 4)

    return g


def another_complex_graph() -> nx.DiGraph:
    """
    Return another complex graph.

    Returns
    -------
    g : nx.Digraph
        A simple graph
    """
    g = nx.DiGraph()

    for i in range(11):
        g.add_node(i, status="ready")

    g.add_edge(0, 2)
    g.add_edge(0, 3)
    g.add_edge(0, 4)

    g.add_edge(1, 5)
    g.add_edge(1, 6)

    g.add_edge(3, 7)
    g.add_edge(3, 8)

    g.add_edge(4, 9)

    g.add_edge(5, 9)

    g.add_edge(8, 10)
    g.add_edge(9, 10)

    return g