"""Contains unittests for the metadata_reader."""


def test_get_all_table_names(yield_metadata_reader, yield_all_metadata):
    """FIXME"""
    table_names = yield_metadata_reader.table_names
    # Extract table names
    all_metadata = yield_all_metadata
    expected_table_names = \
        set(table.split('.')[0] for table in all_metadata.columns)
    assert set(table_names) == expected_table_names


def test_get_table_column_dict(yield_metadata_reader,
                               yield_all_metadata):
    """
    FIXME
    """
    table_columns_dict = yield_metadata_reader.table_column_dict
    # Extract columns dict
    all_metadata = yield_all_metadata
    expected_columns_dict = dict()
    for pandas_col in all_metadata.columns:
        table, col = pandas_col.split('.')
        if table not in expected_columns_dict.keys():
            expected_columns_dict[table] = list()
        expected_columns_dict[table].append(col)

    # Check that the tables are the same
    columns_keys = sorted(table_columns_dict.keys())
    expected_columns_keys = sorted(expected_columns_dict.keys())
    assert set(columns_keys) == set(expected_columns_keys)

    # Check that the columns are the same
    for column_name, expected_column_name in \
            zip(columns_keys, expected_columns_keys):
        columns = table_columns_dict[column_name]
        expected_columns = expected_columns_dict[expected_column_name]
        assert set(columns) == set(expected_columns)


def test_get_table_connections(yield_metadata_reader):
    """FIXME"""
    table_connections = yield_metadata_reader.table_connection
    expected_connections = \
        {'parameters': ('foo',
                        'bar',
                        'baz'),
         'run': ('file_modification',
                 'split',
                 'parameters',
                 'system_info')}
    assert table_connections == expected_connections


def test_get_sorted_columns(yield_metadata_reader,
                            yield_all_metadata):
    """FIXME"""
    sorted_columns = yield_metadata_reader.sorted_columns
    expected = tuple(yield_all_metadata.columns)
    assert sorted_columns == expected


def test_get_parameters_metadata(yield_metadata_reader,
                                 yield_all_metadata):
    """FIXME"""
    # FIXME: The parameter IDs are shown twice...no need for
    #  parameters.foo_id
    tables_to_keep = ('bar', 'baz', 'foo', 'parameters')
    cols_to_keep = [col for col in yield_all_metadata.columns
                    if col.split('.')[0] in tables_to_keep]
    expected = yield_all_metadata.loc[:, cols_to_keep]
    expected.drop_duplicates(inplace=True)
    metadata_reader = yield_metadata_reader
    parameters_metadata = \
        metadata_reader.get_parameters_metadata()
    assert parameters_metadata.equals(expected)


def test_get_all_metadata(yield_metadata_reader, yield_all_metadata):
    """FIXME"""
    expected = yield_all_metadata
    metadata_reader = yield_metadata_reader
    all_metadata = \
        metadata_reader.get_all_metadata()
    assert all_metadata.equals(expected)

