"""Contains unittests for the metadata_reader."""


def test_get_all_table_names(yield_all_table_names):
    """FIXME"""
    table_names = yield_all_table_names
    expected_table_names = ('system_info',
                            'split',
                            'file_modification',
                            'foo',
                            'bar',
                            'baz',
                            'parameters',
                            'run')
    assert set(table_names) == set(expected_table_names)


def test_get_all_column_names(yield_table_column_names):
    """
    FIXME
    """
    columns_dict = yield_table_column_names
    expected_columns_dict = \
        {'system_info': ('id',
                         'machine',
                         'node',
                         'processor',
                         'release',
                         'system',
                         'version'),
         'split': ('id',
                   'number_of_processors',
                   'number_of_nodes',
                   'processors_per_node'),
         'file_modification': ('id',
                               'project_makefile_modified',
                               'project_executable_modified',
                               'project_git_sha',
                               'bout_lib_modified',
                               'bout_git_sha'),
         'foo': ('id',
                 'foo',
                 'bar',
                 'foobar'),
         'bar': ('id',
                 'foo',
                 'qux',
                 'quux'),
         'baz': ('id',
                 'bar',
                 'quuz',
                 'corge'),
         'parameters': ('id',
                        'foo_id',
                        'bar_id',
                        'baz_id'),
         'run': ('id',
                 'name',
                 'submitted_time',
                 'start_time',
                 'stop_time',
                 'latest_status',
                 'file_modification_id',
                 'split_id',
                 'parameters_id',
                 'host_id')}
    columns_keys = columns_dict.keys()
    expected_columns_keys = expected_columns_dict.keys()
    assert set(columns_keys) == set(expected_columns_keys)

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


def test_get_sorted_columns():
    """FIXME"""
    pass