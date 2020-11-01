"""Contains algorithmic functions."""


from typing import List, Optional, Tuple


def get_indices_first_common_element(
    first_list: List, second_list: List
) -> Tuple[Optional[int], Optional[int]]:
    """
    Return the indices in first list where the first common element is found.

    FIXME: Add test and example

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

    References
    ----------
    https://stackoverflow.com/a/16118633/2786884
    """
    for element in second_list:
        if element in first_list:
            return first_list.index(element), second_list.index(element)
    return None, None


def merge_list_at_first_common_element(list_a: List, list_b: List) -> List:
    """
    Merge two lists which are equal after n elements at the first common element.

    FIXME: Add test and example

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
