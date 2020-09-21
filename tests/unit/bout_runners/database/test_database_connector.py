"""Contains unittests for the database connector."""


import sqlite3
from typing import Callable

import pytest
from pathlib import Path

from bout_runners.database.database_connector import DatabaseConnector


def test_db_connector(make_test_database: Callable[[str], DatabaseConnector]) -> None:
    """
    Test the connection.

    This will be done by:
    1. Check that the connection is open
    2. Check that one can execute a statement
    3. Check that it raises error on an invalid statement
    4. Check that it is not possible to change the db_path
    4. Check that it is not possible to change the connection

    Parameters
    ----------
    make_test_database : function
        Function which returns the database connection
    """
    db_connector = make_test_database("connection_test")
    assert isinstance(db_connector.connection, sqlite3.Connection)

    db_connector.execute_statement("CREATE TABLE my_table (col INT)")
    db_connector.execute_statement("SELECT 1+1")

    with pytest.raises(sqlite3.OperationalError):
        db_connector.execute_statement("THIS IS AN INVALID STATEMENT")

    with pytest.raises(AttributeError):
        # Ignoring mypy as db_path is defined as read-only
        db_connector.db_path = Path("invalid")  # type: ignore

    with pytest.raises(AttributeError):
        # Ignoring mypy as db_path is defined as read-only
        db_connector.connection = Path("invalid")  # type: ignore
