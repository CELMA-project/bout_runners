"""Dummy functions used for testing."""


def return_none() -> None:
    """Return None."""
    return None


def return_sum_of_two(number_1: int, number_2: int) -> int:
    """
    Return sum.

    Parameters
    ----------
    number_1 : int
        First part of sum
    number_2 : int
        Second part of sum

    Returns
    -------
    int
        The sum
    """
    return number_1 + number_2


def return_sum_of_three(number_1: int, number_2: int, number_3: int = 0) -> int:
    """
    Return sum.

    Parameters
    ----------
    number_1 : int
        First part of sum
    number_2 : int
        Second part of sum
    number_3 : int
        Third part of sum

    Returns
    -------
    int
        The sum
    """
    return number_1 + number_2 + number_3
