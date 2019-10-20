import logging
import sqlite3
import contextlib
import pandas as pd
from pathlib import Path
from bout_runners.bookkeeper.bookkeeper_utils import \
    obtain_project_parameters
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_system_info_as_sql_type
from bout_runners.bookkeeper.writer import create_table, \
    get_create_table_statement, create_parameter_tables
from bout_runners.runners.runner_utils import run_test_run
from bout_runners.utils.file_operations import get_caller_dir

logger = logging.getLogger(__name__)


def main(project_path=None,
         database_root_path=None):
    """
    FIXME: Global database, on schema per project - find project name

    Using normalized pattern, see [1]_ for a quick overview, and [2]_
    for a slightly deeper explanation

    Parameters
    ----------
    database_root_path : None or Path or str
        Root path of the database file
        If None, the path will be set to $HOME/BOUT_db

    Returns
    -------

    References
    ----------
    [1] https://www.databasestar.com/database-normalization/
    [2] http://www.bkent.net/Doc/simple5.htm
    """

    if project_path is None:
        project_path = get_caller_dir()

    if database_root_path is None:
        # FIXME: Change when going to production
        database_root_path = get_caller_dir()
        # database_root_path = Path().home().joinpath('BOUT_db')

    if not database_root_path.is_dir():
        database_root_path.mkdir(exist_ok=True, parents=True)

    # NOTE: sqlite does not support schemas (except through an
    #       ephemeral ATTACH connection)
    #       https://www.sqlite.org/lang_attach.html
    #       https://stackoverflow.com/questions/30897377/python-sqlite3-create-a-schema-without-having-to-use-a-second-database
    #       Thus we will make one database per project
    schema = project_path.name
    database_path = database_root_path.joinpath(f'{schema}.db')

    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = query(database_path, query_str)

    # Check if tables are created
    if len(table.index) == 0:
        # Create the system info table
        sys_info_dict = get_system_info_as_sql_type()
        sys_info_statement = \
            get_create_table_statement(name='system_info',
                                       columns=sys_info_dict)
        create_table(database_path, sys_info_statement)

        # Create the split table
        split_statement = \
            get_create_table_statement(
                name='split_modification',
                columns={'nodes': 'INTEGER',
                         'processors_per_nodes': 'INTEGER'})
        create_table(database_path, split_statement)

        # Create the file modification table
        file_modification_statement = \
            get_create_table_statement(
                name='file_modification',
                columns={'project_makefile_changed': 'TIMESTAMP',
                         'project_executable_changed': 'TIMESTAMP',
                         'project_git_sha': 'TEXT',
                         'bout_executable_changed': 'TIMESTAMP',
                         'bout_git_sha': 'TEXT'})
        create_table(database_path, file_modification_statement)

        # Create the parameter table
        settings_path = run_test_run(bout_inp_dir, project_path)
        parameter_dict = obtain_project_parameters(settings_path)
        create_parameter_tables(database_path, parameter_dict)

        # Create the run table
        run_statement = \
            get_create_table_statement(
                name='run',
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
        create_table(database_path, run_statement)


# FIXME: Make a sql object which contains query, insert, write etc
#  with member data database_path. Should instance hold connection
#  open? Probably not to avoid concurrency problems
def query(database_path, query_str):
    """
    Makes a query to the database specified in database_path

    Parameters
    ----------
    database_path : Path or str
        Path to database
    query_str : str
        The query to execute

    Returns
    -------
    table : DataFrame
        The result of a query as a DataFrame
    """
    # NOTE: The connection does not close after the 'with' statement
    #       Instead we use the context manager as described here
    #       https://stackoverflow.com/a/47501337/2786884
    # Auto-closes connection

    with contextlib.closing(sqlite3.connect(str(database_path))) as con:
        df = pd.read_sql_query(query_str, con)
    return df


if __name__ == '__main__':
    from dotenv import load_dotenv
    import os

    load_dotenv()
    bout_path = Path(os.getenv('BOUT_PATH')).absolute()

    project_path = bout_path.joinpath('examples', 'conduction')
    bout_inp_dir = project_path.joinpath('data')

    main(project_path)

    settings_path = run_test_run(bout_inp_dir, project_path)

    obtain_project_parameters(settings_path)
    # main()
