"""Contains unittests for the database connector."""

import pytest
import sqlite3


def test_database_connector(make_test_database):
    """
    Test the connection.

    This will be done by:
    1. Check that the connection is open
    2. Check that one can execute a statement
    3. Check that it raises error on an invalid statement
    4. Check that it is not possible to change the database_path

    Parameters
    ----------
    make_test_database : function
        The database from the fixture
    """

    database = make_test_database('connection_test')
    connection = database._DatabaseConnector__connection
    assert isinstance(connection, sqlite3.Connection)

    database.execute_statement('CREATE TABLE my_table (col INT)')
    database.execute_statement('SELECT 1+1')

    with pytest.raises(sqlite3.OperationalError):
        database.execute_statement('THIS IS AN INVALID STATEMENT')

    with pytest.raises(AttributeError):
        database.database_path = 'foo'
