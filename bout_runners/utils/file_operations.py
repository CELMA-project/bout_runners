"""Module containing file operation functions."""


import inspect
from datetime import datetime
from pathlib import Path


def get_caller_dir():
    """
    Return the directory of the topmost caller file.

    Returns
    -------
    caller_dir : Path
        The path of the topmost caller

    References
    ----------
    [1] https://stackoverflow.com/a/1095621/2786884
    """
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    caller_dir = Path(module.__file__).parent

    return caller_dir


def get_modified_time(file_path):
    """
    Return the modification time of a file path.

    Parameters
    ----------
    file_path : Path
        The file path to get the modification time from

    Returns
    -------
    modified_time : str
        The modification time on ISO8601 format

    References
    ----------
    [1] https://stackoverflow.com/a/52858040/2786884
    """
    modified_time = \
        datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()

    return modified_time
