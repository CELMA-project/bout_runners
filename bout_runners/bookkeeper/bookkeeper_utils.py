"""Module containing utils only used in the bookkeeper package."""


import ast
import configparser
import logging
import platform
import re
import subprocess
from pathlib import Path

from bout_runners.utils.file_operations import get_caller_dir
from bout_runners.utils.file_operations import get_modified_time
from bout_runners.utils.paths import get_bout_path
from bout_runners.utils.subprocesses_functions import run_subprocess
from bout_runners.runners.runner_utils import run_settings_run


def obtain_project_parameters(settings_path):
    """
    Return the project parameters from the settings file.

    Parameters
    ----------
    settings_path : Path
        Path to the settings file

    Returns
    -------
    run_parameters_dict : dict
        Dictionary containing the parameters given in BOUT.settings
        On the form
        >>> {'section': {'parameter': 'value'}}

    Notes
    -----
    1. The sectionless part of BOUT.settings will be renamed `global`
    2. In the `global` section, the keys `d` and the directory to the
       BOUT.inp file will be removed
    3. If the section `all` is present in BOUT.settings, the section
       will be renamed `all_boundaries` as `all` is a protected SQL
       keyword
    4. The section `run` will be dropped due to bout_runners own
       `run` table
    """
    # The settings file lacks a header for the global parameter
    # Therefore, we add add the header [global]
    with settings_path.open('r') as settings_file:
        settings_memory = f'[global]\n{settings_file.read()}'

    config = configparser.ConfigParser()
    config.read_string(settings_memory)

    parameter_dict = dict()

    for section in config.sections():
        parameter_dict[section] = dict()
        for key, val in config[section].items():
            # Strip comments
            capture_all_but_comment = '^([^#]*)'
            matches = re.findall(capture_all_but_comment, val, re.M)

            # Exclude comment line
            if len(matches) == 0:
                continue

            # Capitalize in case of boolean
            stripped_val = matches[0].capitalize()

            # If type is not found, type is str
            try:
                val = ast.literal_eval(stripped_val)
            except (SyntaxError, ValueError):
                val = stripped_val

            parameter_dict[section][key] = val

    # NOTE: Bug in .settings: -d path is captured with # not in use
    bout_inp_dir = settings_path.parent
    parameter_dict['global'].pop('d', None)
    parameter_dict['global'].pop(str(bout_inp_dir).lower(), None)

    if 'all' in parameter_dict.keys():
        parameter_dict['all_boundaries'] = parameter_dict.pop('all')

    # Drop run as bout_runners will make its own table with that name
    parameter_dict.pop('run', None)

    return parameter_dict


def cast_parameters_to_sql_type(parameter_dict):
    """
    Return the project parameters from the settings file.

    Parameters
    ----------
    parameter_dict : dict
        Dictionary containing the parameters given in BOUT.settings
        On the form
        >>> {'section': {'parameter': 'value'}}

    Returns
    -------
    parameter_dict_sql_types : dict
        Dictionary containing the parameters given in BOUT.settings
        On the form
        >>> {'section': {'parameter': 'value_type'}}
    """
    type_map = {'bool': 'INTEGER',  # No bool type in SQLite
                'float': 'REAL',
                'int': 'INTEGER',
                'str': 'TEXT'}

    parameter_dict_sql_types = parameter_dict.copy()

    for section in parameter_dict.keys():
        for key, val in parameter_dict[section].items():
            # If type is not found, type is str
            try:
                val_type = type(ast.literal_eval(val))
            except (SyntaxError, ValueError):
                val_type = str

            parameter_dict[section][key] = type_map[val_type.__name__]

    return parameter_dict_sql_types


def get_system_info_as_sql_type():
    """
    Return the SQL types of the system information.

    Returns
    -------
    sys_info_dict : dict
        Dictionary with the attributes of the system as keys and the
        type as values
    """
    attributes = get_system_info()

    sys_info_dict = {att: 'TEXT' for att in attributes}

    return sys_info_dict


def extract_parameters_in_use(project_path,
                              bout_inp_dst_dir,
                              run_parameters_dict):
    """
    Extract parameters that will be used in a run.

    Parameters
    ----------
    project_path : Path
        Root path of project (make file)
    bout_inp_dst_dir : Path
        Path to the directory of BOUT.inp currently in use
    run_parameters_dict : dict of str, dict
        Options on the form
        >>> {'global':{'append': False, 'nout': 5},
        ...  'mesh':  {'nx': 4},
        ...  'section_in_BOUT_inp': {'some_variable': 'some_value'}}

    Returns
    -------
    parameters : dict of str, dict
        Parameters on the same form as `run_parameters_dict`
        (from obtain_project_parameters)
    """
    # Obtain the default parameters
    settings_path = project_path.joinpath('settings_run',
                                          'BOUT.settings')
    if not settings_path.is_file():
        logging.info('No setting files found, running run_settings_run')
        run_settings_run(project_path)
    parameters = obtain_project_parameters(settings_path)
    # Update with parameters from BOUT.inp
    bout_inp_path = bout_inp_dst_dir.joinpath('BOUT.inp')
    parameters.update(obtain_project_parameters(bout_inp_path))
    # Update with parameters from run_parameters_dict
    parameters.update(run_parameters_dict)

    return parameters


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
    try:
        result = run_subprocess('git rev-parse HEAD', path)
        git_sha = result.stdout.decode('utf8').strip()
    # FileNotFoundError when `git` is not found
    except (FileNotFoundError, subprocess.CalledProcessError) as error:
        if isinstance(error, FileNotFoundError):
            error_str = error.args[1]
        elif isinstance(error, subprocess.CalledProcessError):
            error_str = error.args[2]
        logging.warning('Could not retrieve git sha %s', error_str)
        git_sha = 'None'
    return git_sha


def get_system_info():
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
    attributes = tuple(name for name in dir(sys_info)
                       if not name.startswith('_') and not
                       callable(getattr(sys_info, name)))
    return attributes
