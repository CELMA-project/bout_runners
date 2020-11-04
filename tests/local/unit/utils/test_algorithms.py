"""Contains unittests for the algorithms module."""


from typing import Tuple

import pytest
import networkx as nx

from bout_runners.utils.algorithms import get_node_orders
from tests.utils.graphs import simple_graph, complex_graph, another_complex_graph


@pytest.mark.parametrize(
    "graph, reverse, expected",
    [
        (simple_graph(), False, ((0,), (1, 2), (3, 4))),
        (complex_graph(), False, ((0, 1, 12), (2, 11), (3, 4, 5, 6, 7), (8, 9), (10,))),
        (another_complex_graph(), False, ((0, 1), (2, 3, 4, 5, 6), (7, 8, 9), (10,))),
        (simple_graph(), True, ((2, 3, 4), (1,), (0,))),
        (complex_graph(), True, ((3, 5, 10), (8, 9), (4, 6, 7), (2, 11), (0, 1, 12))),
        (another_complex_graph(), True, ((2, 6, 7, 10), (8, 9), (3, 4, 5), (0, 1))),
    ],
)
def test_get_nodes_orders(
    graph: nx.DiGraph, reverse: bool, expected: Tuple[Tuple[int, ...], ...]
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
    result = get_node_orders(graph, reverse)
    assert result == expected
