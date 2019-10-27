import logging
from pathlib import Path
from bout_runners.bookkeeper.bookkeeper_utils import \
    obtain_project_parameters, get_create_table_statement
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_system_info_as_sql_type
from bout_runners.bookkeeper.bookkeeper import Bookkeeper
from bout_runners.runners.runner_utils import run_test_run
from bout_runners.utils.file_operations import get_caller_dir


# FIXME: You are here
# FIXME: Use flake8
# FIXME: Unittests
# FIXME: Utils with paths
# FIXME: Use logging config, and logg contents
logger = logging.getLogger(__name__)


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
    database_root_path : None or Path or str
        Root path of the database file
        If None, the path will be set to $HOME/BOUT_db

    References
    ----------
    [1] https://www.databasestar.com/database-normalization/
    [2] http://www.bkent.net/Doc/simple5.htm
    """
    project_path = Path(project_path)
    database_root_path = Path(database_root_path)

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

    # Create bookkeeper
    bk = Bookkeeper(database_path)

    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bk.query(query_str)

    # Check if tables are created
    if len(table.index) == 0:
        create_system_info_table(bk)
        create_split_table(bk)
        create_file_modification_table(bk)
        create_parameter_tables(bk, bout_inp_dir, project_path)
        create_run_table(bk)


def create_run_table(bk):
    """
    Create a table for the metadata of a run.

    Parameters
    ----------
    bk : Bookkeeper
        A bookkeeper object which doe the sql calls
    """
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
    bk.create_table(run_statement)


def create_parameter_tables(bk, project_path):
    """
    Create one table per section in BOUT.settings and one join table.

    Parameters
    ----------
    bk : Bookkeeper
        A bookkeeper object which doe the sql calls
    project_path : Path
        Path to the project
    """
    settings_path = run_test_run(project_path, bout_inp_dir=None)
    parameter_dict = obtain_project_parameters(settings_path)
    bk.create_parameter_tables(parameter_dict)


def create_file_modification_table(bk):
    """
    Create a table for file modifications.

    Parameters
    ----------
    bk : Bookkeeper
        A bookkeeper object which doe the sql calls
    """
    file_modification_statement = \
        get_create_table_statement(
            name='file_modification',
            columns={'project_makefile_changed': 'TIMESTAMP',
                     'project_executable_changed': 'TIMESTAMP',
                     'project_git_sha': 'TEXT',
                     'bout_executable_changed': 'TIMESTAMP',
                     'bout_git_sha': 'TEXT'})
    bk.create_table(file_modification_statement)


def create_split_table(bk):
    """
    Create a table which stores the grid split.

    Parameters
    ----------
    bk : Bookkeeper
        A bookkeeper object which doe the sql calls
    """
    split_statement = \
        get_create_table_statement(
            name='split_modification',
            columns={'nodes': 'INTEGER',
                     'processors_per_nodes': 'INTEGER'})
    bk.create_table(split_statement)


def create_system_info_table(bk):
    """
    Create a table for the system info.

    Parameters
    ----------
    bk : Bookkeeper
        A bookkeeper object which doe the sql calls
    """
    sys_info_dict = get_system_info_as_sql_type()
    sys_info_statement = \
        get_create_table_statement(name='system_info',
                                   columns=sys_info_dict)
    bk.create_table(sys_info_statement)
