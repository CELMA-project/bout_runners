"""Contains unittests for the metadata_recorder."""


from bout_runners.metadata.metadata_recorder import \
    MetadataRecorder
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.submitter.processor_split import ProcessorSplit


def test_metadata_recorder(yield_bout_path_conduction,
                           get_default_parameters,
                           make_project,
                           make_test_schema,
                           yield_number_of_rows_for_all_tables):
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
    8. Check that it is possible to forcefully make an entry to the
       run table

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
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a
        schema
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
    run_id = \
        metadata_recorder.capture_new_data_from_run(ProcessorSplit())
    assert run_id is None
    # Assert that all the values are 1
    number_of_rows_dict = \
        yield_number_of_rows_for_all_tables(
            metadata_recorder.database_reader)
    assert sum(number_of_rows_dict.values()) == \
        len(number_of_rows_dict.keys())

    # Assert that this is not a new entry
    run_id = \
        metadata_recorder.capture_new_data_from_run(ProcessorSplit())
    assert run_id == 1
    # Assert that all the values are 1
    number_of_rows_dict = \
        yield_number_of_rows_for_all_tables(
            metadata_recorder.database_reader)
    assert sum(number_of_rows_dict.values()) == \
        len(number_of_rows_dict.keys())

    # Create a new entry in the split table
    run_id = \
        metadata_recorder.capture_new_data_from_run(
            ProcessorSplit(number_of_nodes=2))
    # Assert that a new entry has been made (the number of rows in
    # the tables will be counted when checking the forceful entry)
    assert run_id is None

    # Forcefully make an entry
    run_id = \
        metadata_recorder.capture_new_data_from_run(
            ProcessorSplit(number_of_nodes=2), force=True)
    # Assert that a new entry has been made
    assert run_id == 2
    number_of_rows_dict = \
        yield_number_of_rows_for_all_tables(
            metadata_recorder.database_reader)
    tables_with_2 = dict()
    tables_with_2['split'] = number_of_rows_dict.pop('split')
    tables_with_3 = dict()
    tables_with_3['run'] = number_of_rows_dict.pop('run')
    # Assert that all the values are 1
    assert sum(number_of_rows_dict.values()) == \
        len(number_of_rows_dict.keys())
    # Assert that all the values are 2
    assert sum(tables_with_2.values()) == \
        2*len(tables_with_2.keys())
    # Assert that all the values are 3
    assert sum(tables_with_3.values()) == \
        3*len(tables_with_3.keys())
