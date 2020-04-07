"""Contains unittests for the metadata_recorder."""


from bout_runners.metadata_recorder.metadata_recorder import \
    MetadataRecorder
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.submitter.processor_split import ProcessorSplit


def get_number_of_rows_for_all_tables(metadata_recorder):
    """
    Return the number of rows for all tables in a schema.

    Parameters
    ----------
    metadata_recorder : MetadataRecorder
        The object used to capture data from a run

    Returns
    -------
    number_of_rows_dict : dict
        Dict on the form
        >>> {'table_name': int}
    """
    number_of_rows_dict = dict()
    query_str = ("SELECT name FROM sqlite_master\n"
                 "    WHERE type ='table'\n"
                 "    AND name NOT LIKE 'sqlite_%'")
    table_of_tables = metadata_recorder.database_reader.query(query_str)
    for _, table_name_as_series in table_of_tables.iterrows():
        table_name = table_name_as_series['name']
        query_str = f'SELECT COUNT(*) AS rows FROM {table_name}'
        table = metadata_recorder.database_reader.query(query_str)
        number_of_rows_dict[table_name] = table.loc[0, 'rows']
    return number_of_rows_dict


def test_metadata_recorder(yield_bout_path_conduction,
                           get_default_parameters,
                           make_project,
                           make_test_schema):
    """
    Test the metadata recorder.

    Specifically this test will test that:
    1. It is possible to make a new entry
    2. The newly created schema only have 1 row from the created entry
    3. No new entries are made when trying to make the entry again
    4. All the tables still have one row
    5. Create a new entry after changing the run parameters
    6. Check that a new entry is created
    7. See that the `split` table and the `run` table have two rows,
       whereas the rest have one

    Parameters
    ----------
    yield_bout_path_conduction : function
        Function which makes the BoutPaths object for the conduction
        example
    get_default_parameters : DefaultParameters
        The DefaultParameters object
    make_project : Path
        The path to the conduction example
    make_test_schema : function
        The function making the schema (i.e. making all the tables)
    """
    # NOTE: If the project is not made, the metadata recorder will
    # fail when the get_file_modification is trying to get the last
    # edited time of the executable
    _ = make_project
    db_connection, _ = make_test_schema('test_metadata_recorder')
    bout_paths = yield_bout_path_conduction('test_metadata_recorder')
    default_parameters = get_default_parameters
    final_parameters = FinalParameters(default_parameters)

    metadata_recorder = MetadataRecorder(db_connection,
                                         bout_paths,
                                         final_parameters)

    # Assert that this is a new entry
    new_entry = \
        metadata_recorder.capture_new_data_from_run(ProcessorSplit())
    assert new_entry is True
    # Assert that all the values are 1
    number_of_rows_dict = \
        get_number_of_rows_for_all_tables(metadata_recorder)
    assert sum(number_of_rows_dict.values()) == \
        len(number_of_rows_dict.keys())

    # Assert that this is not a new entry
    new_entry = \
        metadata_recorder.capture_new_data_from_run(ProcessorSplit())
    assert new_entry is False
    # Assert that all the values are 1
    number_of_rows_dict = \
        get_number_of_rows_for_all_tables(metadata_recorder)
    assert sum(number_of_rows_dict.values()) == \
        len(number_of_rows_dict.keys())

    # Create a new entry in the split table
    new_entry = \
        metadata_recorder.capture_new_data_from_run(
            ProcessorSplit(number_of_nodes=2))
    # Assert that a new entry has been made
    assert new_entry is True

    number_of_rows_dict = \
        get_number_of_rows_for_all_tables(metadata_recorder)
    tables_with_2 = dict()
    tables_with_2['split'] = number_of_rows_dict.pop('split')
    tables_with_2['run'] = number_of_rows_dict.pop('run')
    # Assert that all the values are 1
    assert sum(number_of_rows_dict.values()) == \
        len(number_of_rows_dict.keys())
    # Assert that all the values are 2
    assert sum(tables_with_2.values()) == \
        2*len(tables_with_2.keys())
