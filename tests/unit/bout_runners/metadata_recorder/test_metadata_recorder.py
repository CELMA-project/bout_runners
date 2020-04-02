"""Contains unittests for the metadata_recorder."""


from bout_runners.metadata_recorder.metadata_recorder import \
    MetadataRecorder
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.submitter.processor_split import ProcessorSplit


# FIXME: You are here: Looks like things are not written to the
#  database...
def test_metadata_recorder(get_bout_path_conduction,
                           get_default_parameters,
                           make_project,
                           make_test_schema):
    # NOTE: If the project is not made, the metadata recorder will
    # fail when the get_file_modification is trying to get the last
    # edited time of the executable
    _ = make_project
    db_connection, _ = make_test_schema('test_metadata_recorder')
    bout_paths = get_bout_path_conduction('test_metadata_recorder')
    default_parameters = get_default_parameters
    final_parameters = FinalParameters(default_parameters)

    metadata_recorder = MetadataRecorder(db_connection,
                                         bout_paths,
                                         final_parameters)

    new_entry = \
        metadata_recorder.capture_new_data_from_run(ProcessorSplit())

    # FIXME: Investigate the writing
    metadata_recorder.database_reader

    # Loop through and check that all tables has only one row
    # Run again and check the same
    # Change ProcessorSplit and check a new entry is made
    # Make join class which enable reading the whole shebang
