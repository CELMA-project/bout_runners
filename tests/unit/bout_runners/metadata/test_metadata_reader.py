"""Contains unittests for the metadata_reader."""


from bout_runners.metadata.metadata_reader import MetadataReader


def test_get_all_table_names(yield_test_db_connection):
    """FIXME: YOU ARE HERE"""
    reader = MetadataReader(yield_test_db_connection)
    table_names = reader.get_all_table_names()
    expected_table_names = ('system_info',
                            'split',
                            'file_modification',
                            'foo',
                            'bar',
                            'baz',
                            'parameters',
                            'run')
    assert set(table_names) == set(expected_table_names)


def test_get_all_column_names(yield_test_db_connection):
    reader = MetadataReader(yield_test_db_connection)
    reader.get_all_table_names()
    a=1