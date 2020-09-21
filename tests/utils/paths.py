"""Contains functions for manipulating paths."""

import contextlib
import os
from pathlib import Path
from typing import Iterator


@contextlib.contextmanager
def change_directory(new_path: Path) -> Iterator[None]:
    """
    Change working directory and return to previous directory on exit.

    Parameters
    ----------
    new_path : Path
        Path to change to

    Yields
    ------
    None
        The function will revert to original directory on exit

    References
    ----------
    [1] https://stackoverflow.com/a/42441759/2786884
    [2] https://stackoverflow.com/a/13197763/2786884
    """
    previous_path = Path.cwd().absolute()
    os.chdir(str(new_path))
    try:
        yield
    finally:
        os.chdir(str(previous_path))
