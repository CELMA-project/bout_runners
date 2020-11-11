"""Contains fixtures for file metadata."""


from pathlib import Path
from typing import Callable, Dict, Iterator, Tuple

import pandas as pd
import pytest
from pandas import DataFrame

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.metadata.metadata_reader import MetadataReader
from bout_runners.metadata.metadata_updater import MetadataUpdater


@pytest.fixture(scope="function")
def yield_number_of_rows_for_all_tables() -> Iterator[
    Callable[[DatabaseReader], Dict[str, int]]
]:
    """
    Yield the function used to count number of rows in a table.

    Yields
    ------
    _get_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a schema
    """

    def _get_number_of_rows_for_all_tables(db_reader: DatabaseReader) -> Dict[str, int]:
        """
        Return the number of rows for all tables in a schema.

        Parameters
        ----------
        db_reader : DatabaseReader
            The object used read from the database

        Returns
        -------
        number_of_rows_dict : dict
            Dict on the form

            >>> {'table_name_1': int, 'table_name_2': int, ...}
        """
        number_of_rows_dict = dict()
        query_str = (
            "SELECT name FROM sqlite_master\n"
            "    WHERE type ='table'\n"
            "    AND name NOT LIKE 'sqlite_%'"
        )
        table_of_tables = db_reader.query(query_str)
        for _, table_name_as_series in table_of_tables.iterrows():
            table_name = table_name_as_series["name"]
            # NOTE: SQL injection possible through bad table name, however the table
            #       names are hard-coded in this example
            query_str = f"SELECT COUNT(*) AS rows FROM {table_name}"  # nosec
            table = db_reader.query(query_str)
            number_of_rows_dict[table_name] = table.loc[0, "rows"]
        return number_of_rows_dict

    yield _get_number_of_rows_for_all_tables


@pytest.fixture(scope="session")
def yield_metadata_reader(get_test_data_path: Path) -> Iterator[MetadataReader]:
    """
    Yield the connection to the test database.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data

    Yields
    ------
    MetadataReader
        The instance to read the metadata
    """
    test_db_connector = DatabaseConnector(name="test", db_root_path=get_test_data_path)
    yield MetadataReader(test_db_connector, drop_id=None)


@pytest.fixture(scope="session")
def yield_all_metadata(get_test_data_path: Path) -> Iterator[DataFrame]:
    """
    Yield the test metadata.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data

    Yields
    ------
    all_metadata : DataFrame
        A DataFrame containing the test metadata
    """
    dates = MetadataReader.date_columns
    all_metadata = pd.read_json(
        get_test_data_path.joinpath("all_metadata.json"),
        orient="split",
        convert_dates=dates,
    )
    yield all_metadata


@pytest.fixture(scope="function")
def get_metadata_updater_and_db_reader(get_test_db_copy: Callable) -> Callable:
    """
    Return an instance of MetadataUpdater.

    The metadata_updater is connected to an isolated database

    Parameters
    ----------
    get_test_db_copy : function
        Function which returns a a database connector to the copy of the test database

    Returns
    -------
    _get_metadata_updater_and_db_reader : function
        Function which returns the MetadataUpdater object with initialized with
        connection to the database and a corresponding DatabaseReader object
    """

    def _get_metadata_updater_and_db_reader(
        name: str,
    ) -> Tuple[MetadataUpdater, DatabaseReader]:
        """
        Return a MetadataUpdater and its DatabaseConnector.

        Parameters
        ----------
        name : str
            Name of the temporary database

        Returns
        -------
        metadata_updater : MetadataUpdater
            Object to update the database with
        db_reader : DatabaseReader
            The corresponding database reader
        """
        db_connector = get_test_db_copy(name)
        db_reader = DatabaseReader(db_connector)
        metadata_updater = MetadataUpdater(db_connector, 1)
        return metadata_updater, db_reader

    return _get_metadata_updater_and_db_reader
