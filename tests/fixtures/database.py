"""Contains fixtures for the databases."""


import shutil
from pathlib import Path
from typing import Iterator, Callable, Optional, Tuple, Dict

import pytest

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.database.database_writer import DatabaseWriter
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters


@pytest.fixture(scope="function", name="make_test_database")
def fixture_make_test_database(
    tmp_path: Path,
) -> Callable[[str], DatabaseConnector]:
    """
    Return the wrapped function for the database connection.

    Parameters
    ----------
    tmp_path: Path
        Temporary path (pytest fixture)

    Returns
    -------
    _make_db : function
        Function making an empty database
    """

    def _make_db(db_name: str) -> DatabaseConnector:
        """
        Make a database.

        It makes sense to have one database per test as we are testing the content of
        the database

        Parameters
        ----------
        db_name : str
            Name of the database

        Returns
        -------
        DatabaseConnector
            The database connection object
        """
        return DatabaseConnector(name=db_name, db_root_path=tmp_path)

    return _make_db


@pytest.fixture(scope="function", name="make_test_schema")
def fixture_make_test_schema(
    get_default_parameters: DefaultParameters,
    make_test_database: Callable[[Optional[str]], DatabaseConnector],
) -> Iterator[
    Callable[[Optional[str]], Tuple[DatabaseConnector, Dict[str, Dict[str, str]]]]
]:
    """
    Return the wrapped function for schema creation.

    Parameters
    ----------
    get_default_parameters : DefaultParameters
        The DefaultParameters object
    make_test_database : function
        Function returning the database connection

    Yields
    ------
    _make_schema : function
        The function making the schema (i.e. making all the tables)
    """

    def _make_schema(
        db_name: Optional[str] = None,
    ) -> Tuple[DatabaseConnector, Dict[str, Dict[str, str]]]:
        """
        Create the schema (i.e. make all the tables) of the database.

        Parameters
        ----------
        db_name : None or str
            Name of the database

        Returns
        -------
        db_connector : DatabaseConnector
            The database connection object
        final_parameters_as_sql_types : dict
            Final parameters as sql types
        """
        db_connector = make_test_database(db_name)

        default_parameters = get_default_parameters
        final_parameters = FinalParameters(default_parameters)
        final_parameters_dict = final_parameters.get_final_parameters()
        final_parameters_as_sql_types = final_parameters.cast_to_sql_type(
            final_parameters_dict
        )

        db_creator = DatabaseCreator(db_connector)

        db_creator.create_all_schema_tables(final_parameters_as_sql_types)

        return db_connector, final_parameters_as_sql_types

    yield _make_schema


@pytest.fixture(scope="function")
def write_to_split(
    make_test_schema: Callable[
        [Optional[str]], Tuple[DatabaseConnector, Dict[str, Dict[str, str]]]
    ],
) -> Iterator[Callable[[Optional[str]], DatabaseConnector]]:
    """
    Return the wrapped function for writing to the split table.

    Parameters
    ----------
    make_test_schema : function
        Function returning the database connection with the schema created

    Yields
    ------
    _write_split : function
        The function writing to the split table
    """

    def _write_split(db_name: Optional[str] = None) -> DatabaseConnector:
        """
        Write to the split table.

        Parameters
        ----------
        db_name : None or str
            Name of the database

        Returns
        -------
        db_connector : DatabaseConnector
            The database connection object
        """
        db_connector, _ = make_test_schema(db_name)

        db_writer = DatabaseWriter(db_connector)
        dummy_split_dict = {
            "number_of_processors": 1,
            "number_of_nodes": 2,
            "processors_per_node": 3,
        }
        db_writer.create_entry("split", dummy_split_dict)

        return db_connector

    yield _write_split


@pytest.fixture(scope="function")
def get_test_db_copy(
    tmp_path: Path,
    get_test_data_path: Path,
    make_test_database: Callable[[Optional[str]], DatabaseConnector],
) -> Callable[[str], DatabaseConnector]:
    """
    Return a function which returns a DatabaseConnector connected to a copy of test.db.

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    get_test_data_path : Path
        Path to test files
    make_test_database : DatabaseConnector
        Database connector to a database located in the temporary
        database directory

    Returns
    -------
    _get_test_db_copy : function
        Function which returns a a database connector to the copy of the test database
    """
    source = get_test_data_path.joinpath("test.db")

    def _get_test_db_copy(name: str) -> DatabaseConnector:
        """
        Return a database connector to the copy of the test database.

        Parameters
        ----------
        name : str
            Name of the temporary database

        Returns
        -------
        db_connector : DatabaseConnector
            DatabaseConnector to the copy of the test database
        """
        destination = tmp_path.joinpath(f"{name}.db")
        shutil.copy(source, destination)
        db_connector = make_test_database(name)
        return db_connector

    return _get_test_db_copy
