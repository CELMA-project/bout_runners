"""Module containing the DatabaseConnector class."""


import logging
import sqlite3
from pathlib import Path
from typing import Optional


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

    def __init__(self, name: str, db_root_path: Path) -> None:
        """
        Set the path to the data base.

        Parameters
        ----------
        name : str
            Name of the database (excluding .db)
        db_root_path : Path or str or None
            Path to database
        """
        # Set the database path
        logging.info("Start: Making a DatabaseConnector object")
        self.__db_path = self.create_db_path(name, db_root_path)
        logging.debug("db_path set to %s", self.db_path)

        # Open the connection
        self.__connection = sqlite3.connect(str(self.db_path))
        logging.info("Done: Making a DatabaseConnector object")

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
    def create_db_path(name: Optional[str], db_root_path: Path) -> Path:
        """
        Create the database path.

        Parameters
        ----------
        name : str
            Name of the database (excluding .db)
        db_root_path : Path
            Path to database

        Returns
        -------
        db_path : Path
            Path to the database
        """
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
