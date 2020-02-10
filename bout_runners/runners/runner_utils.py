"""Module containing utils only used in the runners package."""


import logging
import platform
from bout_runners.runners.base_runner import BoutRunner
from bout_runners.utils.file_operations import get_modified_time
from bout_runners.utils.subprocesses_functions import run_subprocess
from bout_runners.utils.paths import get_bout_path


def run_settings_run(project_path, bout_inp_dir=None):
    """
    Perform a test run.

    Parameters
    ----------
    project_path : Path
        Path to the project
    bout_inp_dir : Path or None
        Path to the BOUT.inp file
        Will be set to `data/` of the `project_path` if not set

    Returns
    -------
    settings_path : Path
        Path to the settings file
    """
    runner = BoutRunner(project_path=project_path)
    runner.set_inp_src(bout_inp_dir)
    runner.set_destination('settings_run')
    logging.info('Performing a run to obtaining settings in %. '
                 'Please do not modify this directory',
                 runner.destination)

    settings_path = runner.destination.joinpath('BOUT.settings')

    if not settings_path.is_file():
        runner.set_split(1)
        runner.set_parameter_dict({'global': {'nout': 0}})
        runner.run()

    return settings_path


def get_file_modification(project_path, makefile_path, exec_name):
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
    file_modification['project_makefile_modified'] =\
        get_modified_time(makefile_path)

    project_executable = makefile_path.parent.joinpath(exec_name)
    file_modification['project_executable_modified'] =\
        get_modified_time(project_executable)

    file_modification['project_git_sha'] = get_git_sha(project_path)

    bout_path = get_bout_path()
    file_modification['bout_lib_modified'] = \
        get_git_sha(bout_path.joinpath('lib', 'libbout++.a'))
    file_modification['bout_git_sha'] = get_git_sha(bout_path)

    return file_modification


def get_git_sha(path):
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
    # FIXME: Make a try statement if subprocess fails
    result = run_subprocess('git rev-parse HEAD', path)
    git_sha = result.stdout.decode('utf8').strip()
    return git_sha


def get_system_info():
    """
    Return the system information.

    Returns
    -------
    attributes : dict
        Dictionary with the attributes of the system

    References
    ----------
    [1]
    https://stackoverflow.com/questions/11637293/iterate-over-object-attributes-in-python
    """
    sys_info = platform.uname()
    attributes = tuple(name for name in dir(sys_info)
                       if not name.startswith('_') and not
                       callable(getattr(sys_info, name)))
    return attributes
