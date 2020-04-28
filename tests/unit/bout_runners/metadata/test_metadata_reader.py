"""Contains unittests for the metadata_reader."""


def test_get_all_table_names(yield_all_table_names, yield_all_metadata):
    """FIXME"""
    table_names = yield_all_table_names
    # Extract table names
    all_metadata = yield_all_metadata
    expected_table_names = \
        set(table.split('.')[0] for table in all_metadata.columns)
    assert set(table_names) == expected_table_names


def test_get_all_column_names(yield_table_column_names,
                              yield_all_metadata):
    """
    FIXME
    """
    columns_dict = yield_table_column_names
    # Extract columns dict
    all_metadata = yield_all_metadata
    expected_columns_dict = dict()
    for pandas_col in all_metadata.columns:
        table, col = pandas_col.split('.')
        if table not in expected_columns_dict.keys():
            expected_columns_dict[table] = list()
        expected_columns_dict[table].append(col)

    # Check that the tables are the same
    columns_keys = sorted(columns_dict.keys())
    expected_columns_keys = sorted(expected_columns_dict.keys())
    assert set(columns_keys) == set(expected_columns_keys)

    # Check that the columns are the same
    for column_name, expected_column_name in \
            zip(columns_keys, expected_columns_keys):
        columns = columns_dict[column_name]
        expected_columns = expected_columns_dict[expected_column_name]
        assert set(columns) == set(expected_columns)


def test_get_table_connections(yield_table_connections):
    """FIXME"""
    table_connections = yield_table_connections
    expected_connections = \
        {'parameters': ('foo',
                        'bar',
                        'baz'),
         'run': ('file_modification',
                 'split',
                 'parameters',
                 'host')}
    assert table_connections == expected_connections


def test_get_sorted_columns(yield_table_column_names,
                            yield_metadata_reader,
                            yield_all_metadata):
    """FIXME"""
    sorted_columns = yield_metadata_reader.get_sorted_columns(
        yield_table_column_names)
    expected = tuple(yield_all_metadata.columns)
    assert sorted_columns == expected


def test_get_parameters_metadata(yield_table_connections,
                                 yield_table_column_names,
                                 yield_metadata_reader,
                                 yield_all_metadata):
    # FIXME: Should store more member data in MetdataReader. It's
    #  easy to use if it's only one method
    table_connections = yield_table_connections
    metadata_reader = yield_metadata_reader
    sorted_columns = yield_metadata_reader.get_sorted_columns(
        yield_table_column_names)
    parameter_connections = {'parameters':
                             table_connections.copy().pop('parameters')}
    parameter_tables = \
        ('parameters', *parameter_connections['parameters'])
    parameter_columns = \
        [col for col in sorted_columns
         if col.split('.')[0] in parameter_tables]

    # FIXME: The parameter IDs are shown twice...no need for
    #  parameters.foo_id
    parameters_metadata = \
        metadata_reader.get_parameters_metadata(
            columns=parameter_columns,
            table_connections=parameter_connections)

    # FIXME: Test is over, but we continue on the full blown stuff
    all_metadata = \
        metadata_reader.get_all_metadata(
            columns=sorted_columns,
            table_connections=table_connections,
            sorted_columns=sorted_columns)
    # FIXME: Fails as None -> NaT and dates
    assert all_metadata.equals(yield_all_metadata)
