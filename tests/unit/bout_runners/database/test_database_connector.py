"""Contains unittests for the database connector."""


import sqlite3
import pytest


def test_database_connector(make_test_database):
    """
    Test the connection.

    This will be done by:
    1. Check that the connection is open
    2. Check that one can execute a statement
    3. Check that it raises error on an invalid statement
    4. Check that it is not possible to change the database_path
    4. Check that it is not possible to change the connection

    Parameters
    ----------
    make_test_database : function
        Function which returns the database connection
    """
    db_connection = make_test_database("connection_test")
    assert isinstance(db_connection.connection, sqlite3.Connection)

    db_connection.execute_statement("CREATE TABLE my_table (col INT)")
    db_connection.execute_statement("SELECT 1+1")

    with pytest.raises(sqlite3.OperationalError):
        db_connection.execute_statement("THIS IS AN INVALID STATEMENT")

    with pytest.raises(AttributeError):
        db_connection.database_path = "invalid"

    with pytest.raises(AttributeError):
        db_connection.connection = "invalid"
