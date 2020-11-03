"""Contains algorithmic functions."""


from typing import List, Optional, Tuple, Any

import networkx as nx

def get_indices_first_common_element(
    first_list: List, second_list: List
) -> Tuple[Optional[int], Optional[int]]:
    """
    Return the indices in first list where the first common element is found.

    Parameters
    ----------
    first_list : list
        The first list
    second_list : list
        The second list

    Returns
    -------
    int or None
        The index in first list where the first common element is found
        None if there are no common elements in first_list and second_list
    int or None
        The index in second list where the first common element is found
        None if there are no common elements in first_list and second_list

    Examples
    --------
    >>> get_indices_first_common_element([0, 1, 2], [3, 2])
    (2, 1)

    References
    ----------
    https://stackoverflow.com/a/16118633/2786884
    """
    for element in second_list:
        if element in first_list:
            return first_list.index(element), second_list.index(element)
    return None, None


def braid_lists(list_a: List, list_b: List) -> List:
    """
    Braid two lists.

    The braid contains of a head, the braid, common elements and a tail

    - Common elements: Consists of the consecutive sequence of common elements
      from the first common element to the last common element in the sequence
    - Tail: Consist of list_a from the last common element in the sequence to the
      end of list_a followed by list_b from the last common element in the sequence
      to the end of list_b if not already present in the braid
    - The braid: Contains alternating elements from list_a and list_b from the first
      common element to the start of the shortest list (counted from the first common
      element to the start)
    - Head: The remainder of the longest list (counted from the first common element
      to the start)

    Parameters
    ----------
    list_a : list
        List to be merged with list_b
    list_b : list
        List to be merged with list_a

    Returns
    -------
    merged_list : list
        The braided list

    Examples
    --------
    FIXME
    >>> braid_lists([5, 8, 2, 1, 9, 7, 3, 4, 6], [518, 12, 1116, 7, 3, 4, 6])
    [5, 8, 518, 2, 12, 1, 1116, 9, 7, 3, 4, 6]
    """
    braided_list = list()
    list_a = list_a.copy()
    list_b = list_b.copy()

    first_element_in_a, first_element_in_b = get_indices_first_common_element(
        list_a, list_b
    )

    if first_element_in_a is None:
        return [*list_a, *list_b]

    # Adding common elements to the list
    additional_common_elements = 0
    for el_a, el_b in zip(list_a[first_element_in_a:], list_b[first_element_in_b:]):
        if el_a != el_b:
            break
        else:
            additional_common_elements += 1

    common_elements = list_a[first_element_in_a:first_element_in_a+additional_common_elements]
    braided_list.extend(common_elements)

    # Adding tail
    braided_list.extend(list_a[first_element_in_a + additional_common_elements:])
    braided_list.extend(el for el in list_b[first_element_in_b + additional_common_elements:] if el_b not in braided_list)

    # Adding braid
    list_a = list_a[:first_element_in_a]
    list_b = list_b[:first_element_in_b]

    # Reverse
    list_a = list_a[::-1]
    list_b = list_b[::-1]

    for el_a, el_b in zip(list_a, list_b):
        braided_list.insert(0, el_a)
        braided_list.insert(0, el_b)

    len_a = len(list_a)
    len_b = len(list_b)

    # Adding head
    abs_diff_a_b = abs(len_a - len_b)
    if abs_diff_a_b != 0:
        if len_a > len_b:
            list_to_insert = list_a[-abs_diff_a_b:]
        else:
            list_to_insert = list_b[-abs_diff_a_b:]

        for index in range(abs_diff_a_b):
            braided_list.insert(0, list_to_insert[index])

    return braided_list


def merge_list_at_first_common_element(list_a: List, list_b: List) -> List:
    """
    Merge two lists which are equal after n elements at the first common element.

    FIXME: Braid superseeds this

    Parameters
    ----------
    list_a : list
        List to be merged with list_b
    list_b : list
        List to be merged with list_a

    Returns
    -------
    merged_list : list
        The merged list

    Examples
    --------
    >>> merge_list_at_first_common_element([5, 8, 2, 1, 9, 7, 3, 4, 6], [518, 12, 1116, 7, 3, 4, 6])
    [5, 8, 2, 1, 9, 518, 12, 1116, 7, 3, 4, 6]
    """
    merged_list = list_a.copy()
    first_element_in_a, first_element_in_b = get_indices_first_common_element(
        list_a, list_b
    )
    if first_element_in_b is None:
        merged_list.extend(list_b)
    else:
        # https://stackoverflow.com/a/7376026/2786884
        merged_list[first_element_in_a:first_element_in_a] = list_b[:first_element_in_b]
    return merged_list


def bfs_nodes(
    graph: nx.DiGraph, node_name: Any, reverse_search: bool = False
) -> Tuple[Any, ...]:
    """
    Return the nodes of a breadth first search.

    Parameters
    ----------
    graph : nx.Digraph
        Graph to search in
    node_name : object
        Name of the node to search from
    reverse_search : bool
        Whether to search reversed in breadth first search

    Returns
    -------
    nodes : tuple of str
        Nodes from the bfs
    """
    edges = nx.bfs_edges(graph, node_name, reverse=reverse_search)
    nodes = [node_name] + [to_node for _, to_node in edges]
    return tuple(nodes)


def get_nodes_in_reversed_in_order(graph: nx.DiGraph,
lowest_order_nodes: Tuple[str, ...], reverse_search: bool = False
) -> Tuple[str, ...]:
    """
    # FIXME: YOU ARE HERE: TEST THIS
    # FIXME: Change names
    Return the reverse sorted list of cluster nodes which has been submitted.

    Parameters
    ----------
    lowest_order_nodes : iterable of str
        Attributes
    reverse_search : bool
        Whether to search reversed in breadth first search

    Returns
    -------
    reverse_sorted_cluster_nodes : tuple of str
        List of node names reverse sorted latest to first order
    """
    successors_of_lower_order_nodes = list()

    for node_name in lowest_order_nodes:
        nodes = bfs_nodes(graph, node_name, reverse_search=reverse_search)
        successors_of_lower_order_nodes.append(nodes)

    if len(successors_of_lower_order_nodes) == 0:
        return tuple()

    braided_successors_of_lower_order_nodes = successors_of_lower_order_nodes.pop(0)
    for predecessors in successors_of_lower_order_nodes:
        braided_successors_of_lower_order_nodes = braid_lists(
            list(braided_successors_of_lower_order_nodes), list(predecessors)
        )

    if reverse_search:
        reverse_sorted_cluster_nodes = braided_successors_of_lower_order_nodes
    else:
        reverse_sorted_cluster_nodes = braided_successors_of_lower_order_nodes[::-1]

    return tuple(reverse_sorted_cluster_nodes)
