"""Module containing the DatabaseConnector class."""


import logging
import sqlite3
from pathlib import Path
from typing import Optional

from bout_runners.utils.file_operations import get_caller_dir


class DatabaseConnector:
    """
    Class creating the database path and executing sql statements.

    Attributes
    ----------
    __db_path : None or Path
        Getter variable for db_path
    __connection : sqlite3.Connection
        Getter variable for connection
    db_path : Path
        Path to database
    connection : sqlite3.Connection
        The connection to the database

    Methods
    -------
    create_db_path(name, db_root_path)
        Create the database path
    execute_statement(sql_statement, *parameters)
        Execute a statement in the database

    Examples
    --------
    >>> database = DatabaseConnector('test')
    >>> database.execute_statement('CREATE TABLE my_table (col INT)')
    """

    def __init__(
        self, name: Optional[str] = None, db_root_path: Optional[Path] = None
    ) -> None:
        """
        Set the path to the data base.

        Parameters
        ----------
        name : str or None
            Name of the database (excluding .db)
            If set to None, the name of the caller directory will be used
        db_root_path : Path or str or None
            Path to database
            If None is set, the path will be set to $HOME/BOUT_db
        """
        # Set the database path
        self.__db_path = self.create_db_path(name, db_root_path)
        logging.info("db_path set to %s", self.db_path)

        # Open the connection
        self.__connection = sqlite3.connect(str(self.db_path))

    def __del__(self) -> None:
        """Close the connection."""
        self.__connection.close()

    @property
    def db_path(self) -> Path:
        """
        Get the properties of self.db_path.

        Returns
        -------
        self.__db_path : Path
            Absolute path to the database

        Notes
        -----
        To avoid corrupting data between databases, the setting this parameter
        outside the constructor is disabled
        """
        return self.__db_path

    @property
    def connection(self) -> sqlite3.Connection:
        """
        Get the properties of self.connection.

        Returns
        -------
        self.__connection : sqlite3.Connection
            The connection to the database

        Notes
        -----
        To avoid corrupting data between databases, the setting this parameter
        outside the constructor is disabled
        """
        return self.__connection

    @staticmethod
    def create_db_path(name: Optional[str], db_root_path: Optional[Path]) -> Path:
        """
        Create the database path.

        Parameters
        ----------
        name : str or None
            Name of the database (excluding .db)
            If set to None, the name of the caller directory will be used
        db_root_path : Path or str or None
            Path to database
            If None is set, the path will be set to $HOME/BOUT_db

        Returns
        -------
        db_path : Path
            Path to the database
        """
        if name is None:
            name = get_caller_dir().name

        if db_root_path is None:
            db_root_path = Path().home().joinpath("BOUT_db")

        db_root_path = Path(db_root_path)

        db_root_path.mkdir(exist_ok=True, parents=True)
        # NOTE: sqlite does not support schemas (except through an
        #       ephemeral ATTACH connection)
        #       Thus we will make one database per project
        # https://www.sqlite.org/lang_attach.html
        # https://stackoverflow.com/questions/30897377/python-sqlite3-create-a-schema-without-having-to-use-a-second-database
        db_path = db_root_path.joinpath(f"{name}.db")

        return db_path

    def execute_statement(self, sql_statement: str, *parameters) -> None:
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
