"""Module containing utils only used in the bookkeeper package."""


import ast
import configparser
import logging
import platform
import re
import subprocess
from pathlib import Path

from bout_runners.utils.file_operations import get_caller_dir, \
    get_modified_time
from bout_runners.utils.paths import get_bout_path
from bout_runners.utils.subprocesses_functions import run_subprocess


def obtain_project_parameters(settings_path):
    """
    Return the project parameters from the settings file.

    Parameters
    ----------
    settings_path : Path
        Path to the settings file

    Returns
    -------
    parameter_dict : dict
        Dictionary containing the parameters given in BOUT.settings
        On the form
        >>> {'section': {'parameter': 'value'}}

    Notes
    -----
    1. The section less part of BOUT.settings will be renamed `global`
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
    parameter_dict.pop('run')

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


def get_create_table_statement(table_name,
                               columns=None,
                               primary_key='id',
                               foreign_keys=None):
    """
    Return a SQL string which can be used to create the table.

    Parameters
    ----------
    table_name : str
        Name of the table
    columns : dict or None
        Dictionary where the key is the column name and the value is
        the type
    primary_key : str
        Name of the primary key (the type is set to INTEGER)
    foreign_keys : dict or None
        Dictionary where the key is the column in this table to be
        used as a foreign key and the value is the tuple
        consisting of (name_of_the_table, key_in_table) to refer to

    Returns
    -------
    create_statement : str
        The SQL statement which creates table
    """
    create_statement = f'CREATE TABLE {table_name} \n('

    create_statement += f'   {primary_key} INTEGER PRIMARY KEY,\n'

    # These are not known during submission time
    nullable_fields = ('start_time',
                       'stop_time')
    if columns is not None:
        for name, sql_type in columns.items():
            create_statement += f'    {name} {sql_type}'
            if name in nullable_fields:
                nullable_str = ',\n'
            else:
                nullable_str = ' NOT NULL,\n'
            create_statement += nullable_str

    if foreign_keys is not None:
        # Create the key as column
        # NOTE: All keys are integers
        for name in foreign_keys.keys():
            create_statement += f'    {name} INTEGER NOT NULL,\n'

        # Add constraint
        for name, (f_table_name, key_in_table) in foreign_keys.items():
            # If the parent is updated or deleted, we would like the
            # same effect to apply to its child (thereby the CASCADE
            # parameter)
            create_statement += \
                (f'    FOREIGN KEY({name}) \n'
                 f'        REFERENCES {f_table_name}({key_in_table})\n'
                 f'            ON UPDATE CASCADE\n'
                 f'            ON DELETE CASCADE,'
                 f'\n')

    # Replace last comma with )
    create_statement = f'{create_statement[:-2]})'

    return create_statement


def get_db_path(database_root_path, project_path):
    """
    Return the database path.

    Parameters
    ----------
    project_path : None or Path or str
        Root path to the project (i.e. where the makefile is located)
        If None the root caller directory will be used
    database_root_path : None or Path or str
        Root path of the database file
        If None, the path will be set to $HOME/BOUT_db

    Returns
    -------
    database_path : Path
        Path to the database
    """
    if project_path is None:
        project_path = get_caller_dir()

    if database_root_path is None:
        # NOTE: This will be changed when going to production
        database_root_path = get_caller_dir()
        # database_root_path = Path().home().joinpath('BOUT_db')

    project_path = Path(project_path)
    database_root_path = Path(database_root_path)

    database_root_path.mkdir(exist_ok=True, parents=True)
    # NOTE: sqlite does not support schemas (except through an
    #       ephemeral ATTACH connection)
    #       Thus we will make one database per project
    # https://www.sqlite.org/lang_attach.html
    # https://stackoverflow.com/questions/30897377/python-sqlite3-create-a-schema-without-having-to-use-a-second-database
    schema = project_path.name
    database_path = database_root_path.joinpath(f'{schema}.db')

    return database_path


def tables_created(bookkeeper):
    """
    Check if the tables is created in the database.

    Parameters
    ----------
    bookkeeper : Bookkeeper
        The Bookkeeper object

    Returns
    -------
    bool
        Whether or not the tables are created
    """
    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')

    table = bookkeeper.query(query_str)
    return len(table.index) != 0


def extract_parameters_in_use(project_path, bout_inp_dir, options):
    """
    Extract parameters that will be used in a run.

    Parameters
    ----------
    project_path : Path
        Root path of project (make file)
    bout_inp_dir : Path
        Path to the directory of BOUT.inp currently in use
    options : dict of str, dict
        Options on the form
        >>> {'global':{'append': False, 'nout': 5},
        ...  'mesh':  {'nx': 4},
        ...  'section_in_BOUT_inp': {'some_variable': 'some_value'}}

    Returns
    -------
    parameters : dict of str, dict
        Parameters on the same form as `parameter_dict`
        (from obtain_project_parameters)
    """
    # Obtain the default parameters
    settings_path = project_path.joinpath('settings_run',
                                          'BOUT.settings')
    if not settings_path.is_file():
        FileNotFoundError(f'{settings_path} not found. It can be '
                          f'created by running '
                          f'bout_runners.runners.run_settings_run')
    parameters = obtain_project_parameters(settings_path)
    # Update with parameters from BOUT.inp
    bout_inp_path = bout_inp_dir.joinpath('BOUT.inp')
    parameters.update(obtain_project_parameters(bout_inp_path))
    # Update with parameters from parameter_dict
    parameters.update(options)

    return parameters


def create_insert_string(field_names, table_name):
    """
    Create a question mark style string for database insertions.

    Values must be provided separately in the execution statement

    Parameters
    ----------
    field_names : array-like
        Names of the fields to populate
    table_name : str
        Name of the table to use for the insertion

    Returns
    -------
    insert_str : str
        The string to be used for insertion
    """
    # From
    # https://stackoverflow.com/a/14108554/2786884
    columns = ', '.join(field_names)
    placeholders = ', '.join('?' * len(field_names))
    insert_str = f'INSERT INTO {table_name} ' \
                 f'({columns}) ' \
                 f'VALUES ({placeholders})'
    return insert_str


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
