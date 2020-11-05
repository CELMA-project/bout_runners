"""Contains functions for manipulating paths."""

import contextlib
import logging
import os
import shutil
from pathlib import Path
from typing import Iterator, Dict


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


class FileStateRestorer:
    """
    Class for restoring files to original state.

    Attributes
    ----------
    __files_to_restore : dict
        Dict of files to restore to original state

    Methods
    -------
    """

    def __init__(self) -> None:
        """Initialize member data."""
        self.__files_to_restore: Dict[Path, Path] = dict()

    def add(self, original_path: Path, force_mark_removal: bool=False) -> None:
        """
        Add file to files to restart in order to restore to original state.

        If a file with the same name is present the file original file will be
        moved to <original_name>_tmp
        This works for both files and directories

        Parameters
        ----------
        original_path : Path
            Path to store as _tmp
        force_mark_removal : bool
            Will mark the original path for removal, even if the path already exists
            Use with care
        """
        if not force_mark_removal and (original_path.is_dir() or original_path.is_file()):
            original_dir = original_path.parent
            original_name = original_path.name
            saved_path = original_dir.joinpath(f"{original_name}_tmp")
            shutil.move(src=str(original_path), dst=str(saved_path))
            logging.debug("Temporary moved %s to %s", original_path, saved_path)
            self.__files_to_restore[original_path] = saved_path
        else:
            logging.debug("Marked %s for removal", original_path)
            self.__files_to_restore[original_path] = original_path

    def restore_files(self) -> None:
        """Loop through the files to save dict and restore files to original state."""
        for original_path, saved_path in self.__files_to_restore.items():
            if original_path.is_dir():
                shutil.rmtree(original_path)
            elif original_path.is_file():
                original_path.unlink()
            else:
                continue
            if original_path != saved_path:
                shutil.move(src=str(saved_path), dst=str(original_path))
                logging.debug(
                    "Removed %s, and moved %s to %s",
                    original_path,
                    saved_path,
                    original_path,
                )
            else:
                logging.debug("Removed %s", original_path)
