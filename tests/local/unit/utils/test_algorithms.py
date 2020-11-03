"""Contains unittests for the algorithms module."""


from typing import List, Tuple

import pytest
import networkx as nx

from bout_runners.utils.algorithms import get_indices_first_common_element, merge_list_at_first_common_element, bfs_nodes, get_nodes_in_reversed_in_order, braid_lists
from tests.utils.graphs import simple_graph, complex_graph, another_complex_graph


@pytest.mark.parametrize(
    "list_a, list_b, expected",
    [([0, 1], [0, 1], (0, 0)),
     ([0, 1], [2, 3], (None, None)),
     ([0, 1, 2], [3, 2], (2, 1)),
     ([3, 2], [0, 1, 2], (1, 2)),
     (["foo", "bar"], ["baz", "foobar", "bar", "bar"], (1, 2))],
)
def test_get_indices_first_common_element(list_a : List, list_b: List, expected: Tuple[int, int]) -> None:
    """
    Test that get_indices_first_common_element works as expected.

    Parameters
    ----------
    list_a : list
        The first list
    list_b : list
        The second list
    expected : tuple
        Expected return tuple
    """
    result = get_indices_first_common_element(list_a, list_b)
    assert expected == result


@pytest.mark.parametrize(
    "list_a, list_b, expected",
    [([0, 1], [0, 1], [0, 1]),
     ([0, 1], [2, 3], [0, 1, 2, 3]),
     ([0, 1, 2], [3, 2], [0, 3, 1, 2]),
     ([3, 2], [0, 1, 2], [0, 1, 3, 2]),
     (["foo", "bar"], ["baz", "foobar", "bar", "bar"], ["baz", "foobar", "foo", "bar", "bar"]),
     (["baz", "foobar", "bar", "bar"], ["foo", "bar"], ["baz", "foo", "foobar", "bar", "bar"]),
     ([5, 8, 2, 1, 9, 7, 3, 4, 6], [518, 12, 1116, 7, 3, 4, 6], [5, 8, 518, 2, 12, 1, 1116, 9, 7, 3, 4, 6]),
     ([518, 12, 1116, 7, 3, 4, 6], [5, 8, 2, 1, 9, 7, 3, 4, 6], [5, 8, 2, 518, 1, 12, 9, 1116, 7, 3, 4, 6]),
     ([0, 1, 2, 3, 4, 5, 6], [7, 8, 9, 10, 2, 3, 11, 12], [7, 8, 9, 0, 10, 1, 2, 3, 4, 5, 6, 11, 12]),
     ([7, 8, 9, 10, 2, 3, 11, 12], [0, 1, 2, 3, 4, 5, 6], [7, 8, 0, 9, 1, 10, 2, 3, 11, 12, 4, 5, 6])
])
def test_braid_lists_before_first_common_element(list_a : List, list_b: List, expected: List) -> None:
    """
    Test that braid_lists works as expected.

    Parameters
    ----------
    list_a : list
        The first list
    list_b : list
        The second list
    expected : tuple
        Expected return tuple
    """
    result = braid_lists(list_a, list_b)
    assert result == expected


# @pytest.mark.parametrize(
#     "list_a, list_b, expected",
#     [([0, 1], [0, 1], [0, 1]),
#      ([0, 1], [2, 3], [0, 1, 2, 3]),
#      ([0, 1, 2], [3, 2], [0, 1, 3, 2]),
#      ([3, 2], [0, 1, 2], [3, 0, 1, 2]),
#      (["foo", "bar"], ["baz", "foobar", "bar", "bar"], ["foo", "baz", "foobar", "bar"]),
#      (["baz", "foobar", "bar", "bar"], ["foo", "bar"], ["baz", "foobar", "foo", "bar", "bar"]),
#      ([5, 8, 2, 1, 9, 7, 3, 4, 6], [518, 12, 1116, 7, 3, 4, 6], [5, 8, 2, 1, 9, 518, 12, 1116, 7, 3, 4, 6]),
#      ([518, 12, 1116, 7, 3, 4, 6], [5, 8, 2, 1, 9, 7, 3, 4, 6], [518, 12, 1116, 5, 8, 2, 1, 9, 7, 3, 4, 6])]
# )
# def test_merge_list_at_first_common_element(list_a : List, list_b: List, expected: List) -> None:
#     """
#     Test that merge_list_at_first_common_element works as expected.
#
#     Parameters
#     ----------
#     list_a : list
#         The first list
#     list_b : list
#         The second list
#     expected : tuple
#         Expected return tuple
#     """
#     result = merge_list_at_first_common_element(list_a, list_b)
#     assert result == expected


@pytest.mark.parametrize(
    "graph, node, reverse_search, expected",
    [(simple_graph(), 0, False, (0, 1, 2, 3, 4)),
     (simple_graph(), 0, True, (0,)),
     (simple_graph(), 4, True, (4, 1, 0)),
     (simple_graph(), 3, True, (3, 1, 0)),
     (complex_graph(), 0, False, (0, 2, 3, 5, 6, 7, 9, 8, 10)),
     (complex_graph(), 1, False, (1, 2, 3, 5, 6, 7, 9, 8, 10)),
     (complex_graph(), 4, False, (4, 9, 8, 10)),
     (complex_graph(), 4, True, (4, 11, 12)),
     (complex_graph(), 6, False, (6, 9, 8, 10)),
     (complex_graph(), 6, True, (6, 2, 0, 1)),
     (another_complex_graph(), 0, False, (0, 2, 3, 4, 7, 8, 9, 10)),
     (another_complex_graph(), 1, False, (1, 5, 6, 9, 10)),
     (another_complex_graph(), 9, False, (9, 10)),
     (another_complex_graph(), 9, True, (9, 4, 5, 0, 1)),
     ]
)
def test_bfs_nodes(graph: nx.DiGraph, node: int, reverse_search: bool, expected: Tuple[int, ...]) -> None:
    """
    Test that bfs_nodes works as expected.

    Parameters
    ----------
    graph : nx.DiGraph
        The graph to search
    node : int
        The node to search from
    reverse_search : bool
        Whether or not to reverse search
    expected : tuple of int
        The expected result
    """
    result = bfs_nodes(graph, node, reverse_search)
    assert result == expected


@pytest.mark.parametrize(
    "graph, lowest_order_nodes, reverse_search, expected",
    [#(simple_graph(), [0], False, (4, 3, 2, 1, 0)),
     #(simple_graph(), [4], True, (4, 1, 0)),
     #(simple_graph(), [4, 2], True, (4, 2, 1, 0)),
     #(simple_graph(), [2, 4], True, (4, 1, 2, 0)),
     #(simple_graph(), [3, 2, 4], True, (3, 4, 2, 1, 0)),
     #(simple_graph(), [4, 2, 3], True, (4, 3, 2, 1, 0)),
     #(complex_graph(), [0], False, (10, 8, 9, 7, 6, 5, 3, 2, 0)),
     #(complex_graph(), [0, 4], False, (10, 8, 9, 7, 4, 6, 5, 3, 2, 0)),
     #(complex_graph(), [4, 0], False, (10, 8, 9, 4, 7, 6, 5, 3, 2, 0)),
     #(complex_graph(), [12, 0], False, (10, 8, 9, 4, 7, 11, 6, 12, 5, 3, 2, 0)),
     #(complex_graph(), [0, 12], False, (10, 8, 9, 7, 4, 6, 11, 5, 12, 3, 2, 0)),
     #(complex_graph(), [0, 1, 12], False, (10, 8, 9, 7, 4, 6, 11, 5, 12, 3, 2, 0, 1)),
     #(another_complex_graph(), [0, 1], False, (10, 9, 8, 6, 7, 5, 4, 1, 3, 2, 0)),
     #(another_complex_graph(), [1, 0], False, (10, 9, 6, 8, 5, 7, 1, 4, 3, 2, 0)),
     #(another_complex_graph(), [10], True, (10, 8, 9, 3, 4, 5, 0, 1)),
     (another_complex_graph(), [7, 10], True, (10, 8, 9, 7, 3, 0, 4, 5, 1))
     ]
)
def test_get_nodes_in_reversed_in_order(graph: nx.DiGraph,
                                        lowest_order_nodes: Tuple[int, ...],
                                        reverse_search: bool,
                                        expected: Tuple[int, ...]) -> None:
    """
    FIXME: Cannot do this, need to find the reverse order

    Test that get_nodes_in_reversed_in_order works as expected.

    Parameters
    ----------
    graph : nx.DiGraph
        The graph to search
    lowest_order_nodes : tuple of int
        The nodes to search from
    reverse_search : bool
        Whether or not to reverse search
    expected : tuple of int
        The expected result
    """
    result = get_nodes_in_reversed_in_order(graph, lowest_order_nodes, reverse_search)
    assert result == expected
