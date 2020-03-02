"""Module containing the Bookkeeper base class."""


import contextlib
import sqlite3
import logging


class BookkeeperBase:
    """
    Class interacting with the database.

    Attributes
    ----------
    database_path : Path or str
        Path to database
    FIXME: Upate these

    Methods
    -------
    create_table(sql_statement)
        Create a table for each BOUT.settings section and a join table
    query(query_str)
        Make a query to the database
    """

    # FIXME: You are here: Ripping apart Bookkeeper, added from
    #  bookkeeper_utils and creator - remember to move the tests
    # FIXME: Seems like no need to make abstract classes (except for
    #  runner)
    def __init__(self, database_path):
        """
        Set the path to the data base.

        Parameters
        ----------
        database_path : Path or str
            Path to database
        """
        self.database_path = database_path
        logging.info('Database path set to %s', self.database_path)

    def execute_statement(self, sql_statement, *parameters):
        """
        Execute a statement in the database.

        Parameters
        ----------
        sql_statement : str
            The statement execute
        parameters : tuple
            Parameters used in .execute of the cursor (like )
        """
        # NOTE: The connection does not close after the 'with' statement
        #       Instead we use the context manager as described here
        #       https://stackoverflow.com/a/47501337/2786884
        # Auto-closes connection
        with contextlib.closing(sqlite3.connect(
                str(self.database_path))) as context:
            # Auto-commits
            with context as con:
                # Auto-closes cursor
                with contextlib.closing(con.cursor()) as cur:
                    # Check if tables are present
                    cur.execute(sql_statement, parameters)

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


