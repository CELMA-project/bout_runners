"""Contains modules to create database and tables."""


import logging
from pathlib import Path
from bout_runners.bookkeeper.bookkeeper_utils import \
    obtain_project_parameters
from bout_runners.bookkeeper.bookkeeper_utils import get_db_path
from bout_runners.bookkeeper.bookkeeper_utils import tables_created
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_create_table_statement
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_system_info_as_sql_type
from bout_runners.bookkeeper.bookkeeper import Bookkeeper
from bout_runners.runners.runner_utils import run_settings_run


def create_database(project_path=None,
                    database_root_path=None):
    """
    Create the database if it doesn't already exist.

    The database will be on normalized form, see [1]_ for a quick
    overview, and [2]_ for a slightly deeper explanation

    Parameters
    ----------
    project_path : None or Path or str
        Root path to the project (i.e. where the makefile is located)
        If None the root caller directory will be used
    database_root_path : None or Path or str
        Root path of the database file
        If None, the path will be set to $HOME/BOUT_db

    References
    ----------
    [1] https://www.databasestar.com/database-normalization/
    [2] http://www.bkent.net/Doc/simple5.htm
    """
    logging.info('Preparing database')
    database_path = get_db_path(database_root_path, project_path)

    # Create bookkeeper
    bookkeeper = Bookkeeper(database_path)

    # Check if tables are created
    if tables_created(bookkeeper):
        create_system_info_table(bookkeeper)
        create_split_table(bookkeeper)
        create_file_modification_table(bookkeeper)
        create_parameter_tables(bookkeeper, project_path)
        create_run_table(bookkeeper)

    logging.info('Database ready')


def create_run_table(bookkeeper):
    """
    Create a table for the metadata of a run.

    Parameters
    ----------
    bookkeeper : Bookkeeper
        A bookkeeper object which doe the sql calls
    """
    run_statement = \
        get_create_table_statement(
            table_name='run',
            columns={'name': 'TEXT',
                     'start': 'TIMESTAMP',
                     'stop': 'TIMESTAMP',
                     'status': 'TEXT',
                     },
            foreign_keys={'file_modification_id':
                          ('file_modification', 'id'),
                          'split_id': ('split', 'id'),
                          'parameters_id': ('parameters', 'id'),
                          'host_id': ('host', 'id')})
    bookkeeper.create_table(run_statement)


def create_parameter_tables(bookkeeper, project_path):
    """
    Create one table per section in BOUT.settings and one join table.

    Parameters
    ----------
    bookkeeper : Bookkeeper
        A bookkeeper object which doe the sql calls
    project_path : Path
        Path to the project
    """
    settings_path = run_settings_run(project_path, bout_inp_dir=None)
    parameter_dict = obtain_project_parameters(settings_path)
    bookkeeper.create_parameter_tables(parameter_dict)


def create_file_modification_table(bookkeeper):
    """
    Create a table for file modifications.

    Parameters
    ----------
    bookkeeper : Bookkeeper
        A bookkeeper object which doe the sql calls
    """
    file_modification_statement = \
        get_create_table_statement(
            table_name='file_modification',
            columns={'project_makefile_changed': 'TIMESTAMP',
                     'project_executable_changed': 'TIMESTAMP',
                     'project_git_sha': 'TEXT',
                     'bout_executable_changed': 'TIMESTAMP',
                     'bout_git_sha': 'TEXT'})
    bookkeeper.create_table(file_modification_statement)


def create_split_table(bookkeeper):
    """
    Create a table which stores the grid split.

    Parameters
    ----------
    bookkeeper : Bookkeeper
        A bookkeeper object which doe the sql calls
    """
    split_statement = \
        get_create_table_statement(
            table_name='split',
            columns={'nodes': 'INTEGER',
                     'processors_per_nodes': 'INTEGER'})
    bookkeeper.create_table(split_statement)


def create_system_info_table(bookkeeper):
    """
    Create a table for the system info.

    Parameters
    ----------
    bookkeeper : Bookkeeper
        A bookkeeper object which doe the sql calls
    """
    sys_info_dict = get_system_info_as_sql_type()
    sys_info_statement = \
        get_create_table_statement(table_name='system_info',
                                   columns=sys_info_dict)
    bookkeeper.create_table(sys_info_statement)
