"""Module containing file operation functions."""


import logging
import shutil
from typing import Union
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


def copy_restart_files(
    copy_restart_from: Union[str, Path], copy_restart_to: Union[str, Path]
) -> None:
    """
    Copy restart files.

    Parameters
    ----------
    copy_restart_from : str or Path
        Directory to copy restart files from
    copy_restart_to : str or Path
        Directory to copy restart files to

    Raises
    ------
    FileNotFoundError
        In case no restart files are found
    """
    copy_restart_from = Path(copy_restart_from)
    copy_restart_to = Path(copy_restart_to)
    src_list = list(copy_restart_from.glob("BOUT.restart.*"))
    if len(src_list) == 0:
        raise FileNotFoundError(f"No restart files found in {copy_restart_from}")
    for src in src_list:
        dst = copy_restart_to.joinpath(src.name)
        shutil.copy(src, dst)
        logging.debug("Copied %s to %s", src, dst)
