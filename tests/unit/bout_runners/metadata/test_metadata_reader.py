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


def test_get_sorted_columns(yield_table_column_names,
                            yield_metadata_reader):
    """FIXME"""
    sorted_columns = yield_metadata_reader.get_sorted_columns(
        yield_table_column_names)
    expected = \
        ('run.id',
         'run.file_modification_id',
         'run.host_id',
         'run.latest_status',
         'run.name',
         'run.parameters_id',
         'run.split_id',
         'run.start_time',
         'run.stop_time',
         'run.submitted_time',
         'bar.id',
         'bar.foo',
         'bar.quux',
         'bar.qux',
         'baz.id',
         'baz.bar',
         'baz.corge',
         'baz.quuz',
         'file_modification.id',
         'file_modification.bout_git_sha',
         'file_modification.bout_lib_modified',
         'file_modification.project_executable_modified',
         'file_modification.project_git_sha',
         'file_modification.project_makefile_modified',
         'foo.id',
         'foo.bar',
         'foo.foo',
         'foo.foobar',
         'parameters.id',
         'parameters.bar_id',
         'parameters.baz_id',
         'parameters.foo_id',
         'split.id',
         'split.number_of_nodes',
         'split.number_of_processors',
         'split.processors_per_node',
         'system_info.id',
         'system_info.machine',
         'system_info.node',
         'system_info.processor',
         'system_info.release',
         'system_info.system',
         'system_info.version')
    assert sorted_columns == expected


def test_get_parameters_metadata(yield_table_connections,
                                 yield_table_column_names,
                                 yield_metadata_reader):
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
    all_metadata_query = \
        metadata_reader.get_all_metadata(
            columns=sorted_columns,
            table_connections=table_connections,
            sorted_columns=sorted_columns)
    a=1
