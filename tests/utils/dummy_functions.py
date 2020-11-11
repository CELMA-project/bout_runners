"""Dummy functions used for testing."""


import logging
import shutil
from pathlib import Path
from typing import Optional, Tuple


def return_none(*args: Optional[Tuple], **kwargs: Optional[dict]) -> None:
    """
    Return None.

    Parameters
    ----------
    args : tuple
        Any positional arguments
    kwargs : dict
        Any keyword arguments
    """
    logging.debug("args: %s, kwargs: %s", args, kwargs)


def mock_expand(*args: Optional[Tuple], **kwargs: Optional[dict]) -> None:
    """
    Mock the expand function.

    Parameters
    ----------
    args : tuple
        Any positional arguments
    kwargs : dict
        Any keyword arguments
    """
    logging.debug("args: %s, kwargs: %s", args, kwargs)
    # NOTE: We are ignoring the types as this is a dummy mock function
    in_dir = Path(kwargs["path"])  # type: ignore
    out_dir = Path(kwargs["output"])  # type: ignore
    out_dir.mkdir()
    src_restart_files = list(in_dir.glob("*.restart.*"))
    for src in src_restart_files:
        dst = out_dir.joinpath(src.name)
        logging.debug("Copied from %s to %s", src, dst)
        shutil.copy(src, dst)


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
