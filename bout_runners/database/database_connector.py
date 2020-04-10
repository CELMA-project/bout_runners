"""Module containing the DatabaseBase class."""


import sqlite3
import logging
from pathlib import Path
from bout_runners.utils.file_operations import get_caller_dir


class DatabaseConnector:
    """
    Class creating the database path and executing sql statements.

    Attributes
    ----------
    __database_path : None or Path
        Getter variable for database_path
    __connection : Connection
        Getter variable for connection
    database_path : Path
        Path to database
    connection : Connection
        The connection to the database

    Methods
    -------
    create_db_path(name, database_root_path)
        Create the database path
    execute_statement(sql_statement, *parameters)
        Execute a statement in the database

    Examples
    --------
    >>> database = DatabaseConnector('test')
    >>> database.execute_statement('CREATE TABLE my_table (col INT)')
    """

    def __init__(self, name=None, database_root_path=None):
        """
        Set the path to the data base.

        Parameters
        ----------
        name : str or None
            Name of the database (excluding .db)
            If set to None, the name of the caller directory will be
            used
        database_root_path : Path or str or None
            Path to database
            If None is set, the path will be set to $HOME/BOUT_db
        """
        # Declare variables to be used in the getters and setters
        self.__database_path = None

        # Set the database path
        self.__database_path = \
            self.create_db_path(name, database_root_path)
        logging.info('database_path set to %s', self.database_path)

        # Open the connection
        self.__connection = sqlite3.connect(str(self.database_path))

    def __del__(self):
        """Close the connection."""
        self.__connection.close()

    @property
    def database_path(self):
        """
        Set the properties of self.database_path.

        Returns
        -------
        self.__database_path : Path
            Absolute path to the database

        Notes
        -----
        To avoid corrupting data between databases, the setting this
        parameter outside the constructor is disabled
        """
        return self.__database_path

    @property
    def connection(self):
        """
        Set the properties of self.connection.

        Returns
        -------
        self.__connection : Connection
        The connection to the database

        Notes
        -----
        To avoid corrupting data between databases, the setting this
        parameter outside the constructor is disabled
        """
        return self.__connection

    @staticmethod
    def create_db_path(name, database_root_path):
        """
        Create the database path.

        Parameters
        ----------
        name : str or None
            Name of the database (excluding .db)
            If set to None, the name of the caller directory will be
            used
        database_root_path : Path or str or None
            Path to database
            If None is set, the path will be set to $HOME/BOUT_db

        Returns
        -------
        database_path : Path
            Path to the database
        """
        if name is None:
            name = get_caller_dir().name

        if database_root_path is None:
            database_root_path = Path().home().joinpath('BOUT_db')

        database_root_path = Path(database_root_path)

        database_root_path.mkdir(exist_ok=True, parents=True)
        # NOTE: sqlite does not support schemas (except through an
        #       ephemeral ATTACH connection)
        #       Thus we will make one database per project
        # https://www.sqlite.org/lang_attach.html
        # https://stackoverflow.com/questions/30897377/python-sqlite3-create-a-schema-without-having-to-use-a-second-database
        database_path = database_root_path.joinpath(f'{name}.db')

        return database_path

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
        cursor = self.__connection.cursor()
        cursor.execute(sql_statement, parameters)
        self.__connection.commit()
