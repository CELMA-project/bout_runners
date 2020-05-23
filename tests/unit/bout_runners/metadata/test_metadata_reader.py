"""Contains unittests for the metadata_reader."""


from typing import Dict, List

from bout_runners.metadata.metadata_reader import MetadataReader
from pandas import DataFrame


def test_get_table_column_dict(
    yield_metadata_reader: MetadataReader, yield_all_metadata: DataFrame
) -> None:
    """
    Test that the metadata reader retrieves the table column dict.

    Parameters
    ----------
    yield_metadata_reader : MetadataReader
        The metadata reader object
    yield_all_metadata : DataFrame
        The test metadata
    """
    table_columns_dict = yield_metadata_reader.table_column_dict
    # Extract columns dict
    all_metadata = yield_all_metadata
    expected_columns_dict: Dict[str, List[str]] = dict()
    for pandas_col in all_metadata.columns:
        table, col = pandas_col.split(".")
        if table not in expected_columns_dict.keys():
            expected_columns_dict[table] = list()
        expected_columns_dict[table].append(col)

    # Check that the tables are the same
    columns_keys = sorted(table_columns_dict.keys())
    expected_columns_keys = sorted(expected_columns_dict.keys())
    assert set(columns_keys) == set(expected_columns_keys)

    # Check that the columns are the same
    for column_name, expected_column_name in zip(columns_keys, expected_columns_keys):
        columns = table_columns_dict[column_name]
        expected_columns = expected_columns_dict[expected_column_name]
        assert set(columns) == set(expected_columns)


def test_get_table_connections(yield_metadata_reader: MetadataReader) -> None:
    """
    Test that the metadata reader retrieves the table connections.

    Parameters
    ----------
    yield_metadata_reader : MetadataReader
        The metadata reader object
    """
    table_connections = yield_metadata_reader.table_connection
    expected_connections = {
        "parameters": ("foo", "bar", "baz"),
        "run": ("file_modification", "split", "parameters", "system_info"),
    }
    assert table_connections == expected_connections


def test_get_sorted_columns(
    yield_metadata_reader: MetadataReader, yield_all_metadata: DataFrame
) -> None:
    """
    Test that the metadata reader retrieves sorted columns.

    Parameters
    ----------
    yield_metadata_reader : MetadataReader
        The metadata reader object
    yield_all_metadata : DataFrame
        The test metadata
    """
    sorted_columns = yield_metadata_reader.sorted_columns
    expected = tuple(yield_all_metadata.columns)
    assert sorted_columns == expected


def test_get_parameters_metadata(
    yield_metadata_reader: MetadataReader, yield_all_metadata: DataFrame
) -> None:
    """
    Test that the metadata reader can get the parameters metadata.

    Parameters
    ----------
    yield_metadata_reader : MetadataReader
        The metadata reader object
    yield_all_metadata : DataFrame
        The test metadata
    """
    tables_to_keep = ("bar", "baz", "foo", "parameters")
    cols_to_keep = [
        col for col in yield_all_metadata.columns if col.split(".")[0] in tables_to_keep
    ]
    expected = yield_all_metadata.loc[:, cols_to_keep]
    expected.drop_duplicates(inplace=True)
    metadata_reader = yield_metadata_reader
    parameters_metadata = metadata_reader.get_parameters_metadata()
    assert parameters_metadata.equals(expected)


def test_get_all_metadata(
    yield_metadata_reader: MetadataReader, yield_all_metadata: DataFrame
) -> None:
    """
    Test that the metadata reader and the drop_id decorator works.

    Parameters
    ----------
    yield_metadata_reader : MetadataReader
        The metadata reader object
    yield_all_metadata : DataFrame
        The test metadata
    """
    expected = yield_all_metadata
    metadata_reader = yield_metadata_reader
    all_metadata = metadata_reader.get_all_metadata()
    assert all_metadata.equals(expected)

    metadata_reader.drop_id = "parameters"
    no_parameters_df = metadata_reader.get_all_metadata()
    no_parameter_columns = [
        col for col in tuple(expected.columns) if not col.startswith("parameters.")
    ]
    assert list(no_parameters_df.columns) == no_parameter_columns

    metadata_reader.drop_id = "keep_run_id"
    only_run_id_df = metadata_reader.get_all_metadata()
    only_run_id_columns = [
        col
        for col in no_parameter_columns
        if not ((col.endswith(".id") and not col == "run.id") or col.endswith("_id"))
    ]
    assert list(only_run_id_df.columns) == only_run_id_columns

    metadata_reader.drop_id = "all_id"
    all_id_df = metadata_reader.get_all_metadata()
    all_id = [col for col in only_run_id_columns if not col == "run.id"]
    assert list(all_id_df.columns) == all_id
