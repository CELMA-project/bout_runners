"""Module containing file operation functions."""


from datetime import datetime
from pathlib import Path


def get_caller_dir() -> Path:
    """
    Return the directory of the topmost caller file.

    Returns
    -------
    caller_dir : Path
        The path of the topmost caller
    """
    caller_dir = Path().cwd().absolute()

    return caller_dir


def get_modified_time(file_path: Path) -> str:
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
    """
    # From
    # https://stackoverflow.com/a/52858040/2786884
    modified_time = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()

    return modified_time
