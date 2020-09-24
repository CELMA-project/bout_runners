"""Module containing database utils."""


import logging
import platform
from pathlib import Path

# NOTE: subprocess can be vulnerable if shell=True
#       However, CalledProcessError has no known security vulnerabilities
from subprocess import CalledProcessError  # nosec
from typing import Dict

from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.utils.file_operations import get_modified_time
from bout_runners.utils.paths import get_bout_directory


def get_system_info_as_sql_type() -> Dict[str, str]:
    """
    Return the SQL types of the system information.

    Returns
    -------
    sys_info_dict : dict
        Dictionary with the attributes of the system as keys and the type as values
    """
    attributes = get_system_info()

    sys_info_dict = {att: "TEXT" for att in attributes.keys()}

    return sys_info_dict


def get_file_modification(
    project_path: Path, makefile_path: Path, exec_name: str
) -> Dict[str, str]:
    """
    Return the file modification info.

    Parameters
    ----------
    project_path : Path
        Path to the project
    makefile_path : Path
            Path to the project makefile
    exec_name : str
        Name of the executable

    Returns
    -------
    file_modification : dict
        The file modification on the form

        >>> {'project_makefile_modified': str,
        ...  'project_executable_modified': str,
        ...  'project_git_sha': str,
        ...  'bout_lib_modified': str,
        ...  'bout_git_sha': str,}
    """
    file_modification = dict()
    file_modification["project_makefile_modified"] = get_modified_time(makefile_path)

    project_executable = makefile_path.parent.joinpath(exec_name)
    file_modification["project_executable_modified"] = get_modified_time(
        project_executable
    )

    file_modification["project_git_sha"] = get_git_sha(project_path)

    bout_path = get_bout_directory()
    file_modification["bout_lib_modified"] = get_modified_time(
        bout_path.joinpath("lib", "libbout++.a")
    )
    file_modification["bout_git_sha"] = get_git_sha(bout_path)

    return file_modification


def get_git_sha(path: Path) -> str:
    """
    Return the git hash.

    Parameters
    ----------
    path : Path
        Path to query the git hash

    Returns
    -------
    git_sha : str
        The git hash
    """
    try:
        submitter = LocalSubmitter(path)
        submitter.submit_command("git rev-parse HEAD")
        submitter.wait_until_completed()
        git_sha = submitter.std_out
    # FileNotFoundError when `git` is not found
    except (FileNotFoundError, CalledProcessError) as error:
        if isinstance(error, FileNotFoundError):
            error_str = error.args[1]
        elif isinstance(error, CalledProcessError):
            error_str = error.args[2]
        else:
            error_str = "Unknown error"
        logging.warning("Could not retrieve git sha: %s", error_str)
        git_sha = "None"

    git_sha = git_sha if git_sha is not None else "None"

    return git_sha


def get_system_info() -> Dict[str, str]:
    """
    Return the system information.

    Returns
    -------
    attributes : dict
        Dictionary with the attributes of the system
    """
    # From
    # https://stackoverflow.com/questions/11637293/iterate-over-object-attributes-in-python
    sys_info = platform.uname()
    attributes = {
        name: getattr(sys_info, name)
        for name in dir(sys_info)
        if not name.startswith("_") and not callable(getattr(sys_info, name))
    }
    return attributes
