"""Contains functions for checking runs."""
from typing import Callable, Dict

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.executor.bout_paths import BoutPaths


def assert_first_run(
    bout_paths: BoutPaths, db_connection: DatabaseConnector
) -> DatabaseReader:
    """
    Assert that the first run went well.

    Parameters
    ----------
    bout_paths : BoutPaths
        The object containing the paths
    db_connection : DatabaseConnector
        The database connection

    Returns
    -------
    db_reader : DatabaseReader
        The database reader object
    """
    db_reader = DatabaseReader(db_connection)
    assert (
        bout_paths.bout_inp_dst_dir.joinpath("BOUT.dmp.0.nc").is_file()
        or bout_paths.bout_inp_dst_dir.joinpath("BOUT.dmp.0.h5").is_file()
    )
    assert db_reader.check_tables_created()
    return db_reader


def assert_tables_has_len_1(
    db_reader: DatabaseReader,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
) -> None:
    """
    Assert that tables has length 1.

    Parameters
    ----------
    db_reader : DatabaseReader
        The database reader object
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a
        schema
    """
    number_of_rows_dict = yield_number_of_rows_for_all_tables(db_reader)
    assert sum(number_of_rows_dict.values()) == len(number_of_rows_dict.keys())


def assert_force_run(
    db_reader: DatabaseReader,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
) -> None:
    """
    Assert that the force run is effective.

    Parameters
    ----------
    db_reader : DatabaseReader
        The database reader object
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a
        schema
    """
    number_of_rows_dict = yield_number_of_rows_for_all_tables(db_reader)
    tables_with_2 = dict()
    tables_with_2["run"] = number_of_rows_dict.pop("run")
    # Assert that all the values are 1
    assert sum(number_of_rows_dict.values()) == len(number_of_rows_dict.keys())
    # Assert that all the values are 2
    assert sum(tables_with_2.values()) == 2 * len(tables_with_2.keys())
