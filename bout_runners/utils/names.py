"""Module containing functions to extract names."""


import logging
from pathlib import Path
from typing import Optional

from bout_runners.make.read_makefile import (
    BoutMakefileVariableReader,
    MakefileReaderError,
)


def get_exec_name(makefile_path: Path) -> str:
    """
    Return the name of the project executable.

    This method first searches for the 'TARGET' variable in the makefile. If not
    found it infers the name from the 'SOURCEC' variable.

    Parameters
    ----------
    makefile_path : Path or str
        Path to the make file

    Returns
    -------
    exec_name : str
        Name of the executable
    """
    try:
        logging.info("Trying to read TARGET from makefile")
        exec_name = BoutMakefileVariableReader(makefile_path, "TARGET").value
    except MakefileReaderError:
        logging.info("Could not read TARGET from makefile, will infer from SOURCEC")
        exec_name = BoutMakefileVariableReader(makefile_path, "SOURCEC").value
        # Strip the name from the last .c*
        split_by = ".c"
        split_list = exec_name.split(split_by)
        split_to_join = split_list if len(split_list) == 1 else split_list[:-1]
        exec_name = f"{split_by}".join(split_to_join)

    return exec_name


def get_makefile_path(makefile_root_path: Path, makefile_name: Optional[str]) -> Path:
    """
    Return the makefile path.

    Parameters
    ----------
    makefile_root_path : Path or str
        Root path of make file
    makefile_name : None or str
        The name of the makefile.
        If set to None, it tries the following names, in order:
        'GNUmakefile', 'makefile' and 'Makefile'

    Returns
    -------
    makefile_path : Path
        Path to the makefile
    """
    if makefile_name is None:
        makefile_name = get_makefile_name(makefile_root_path)
    makefile_path = Path(makefile_root_path).joinpath(makefile_name)
    return makefile_path


def get_makefile_name(makefile_root_path: Path) -> str:
    """
    Search for a valid Makefile.

    The order of the search is 'GNUmakefile', 'makefile' and 'Makefile'

    Parameters
    ----------
    makefile_root_path : Path
        Path to the root directory of the Makefile

    Returns
    -------
    makefile_name : str
        The name of the Makefile

    Raises
    ------
    FileNotFoundError
        If none of the valid makefile names are found
    """
    possible_names = ("GNUmakefile", "Makefile", "makefile")

    makefile_name = None

    for name in possible_names:
        if makefile_root_path.joinpath(name).is_file():
            makefile_name = name
            break

    if makefile_name is None:
        msg = (
            f"Could not find a valid Makefile name in {makefile_root_path}. "
            f"Valid Makefile names are {' ,'.join(possible_names)}"
        )
        logging.critical(msg)
        raise FileNotFoundError(msg)

    return makefile_name
