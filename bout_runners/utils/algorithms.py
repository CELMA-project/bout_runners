"""Contains algorithmic functions."""


from typing import Tuple
import networkx as nx


def get_node_orders(
    graph: nx.DiGraph, reverse: bool = False
) -> Tuple[Tuple[str, ...], ...]:
    """
    Return nodes sorted at order.

    FIXME: Merge this with run_graph

    One order is considered as the nodes without any in edges
    To find the next order remove the first order from the graph and
    repeat the first step

    Warnings
    --------
    As we are counting an order from the nodes with no in edges the
    result of
    >>> get_node_orders(reverse=True) != get_node_orders()[::-1]

    Parameters
    ----------
    graph : nx.Digraph
        Graph to get the orders from
    reverse : bool
        Whether or not to reverse the graph before finding the orders

    Returns
    -------
    orders : tuple of tuple of str
        A tuple of tuple where the innermost tuple constitutes an order
    """
    graph_copy = graph.copy()
    if reverse:
        # NOTE: Not possible to make a deepcopy as some of the attributes
        #       of the nodes are not pickable (example sqlite3.connection)
        #       Thus we set copy=False
        # https://networkx.org/documentation/stable/reference/classes/generated/networkx.DiGraph.reverse.html
        graph_copy = graph_copy.reverse(copy=False)
        # NOTE: As copy=False, the resulting graph is frozen
        #       To unfreeze we make a new object
        # https://networkx.org/documentation/stable/reference/generated/networkx.classes.function.freeze.html
        graph_copy = nx.DiGraph(graph_copy)
    orders = list()
    while len(graph_copy.nodes) != 0:
        current_roots = tuple(
            node for node, degree in graph_copy.in_degree() if degree == 0
        )
        orders.append(tuple(current_roots))
        graph_copy.remove_nodes_from(current_roots)

    return tuple(orders)
