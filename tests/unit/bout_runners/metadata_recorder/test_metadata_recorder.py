"""Contains unittests for the metadata_recorder."""


from bout_runners.metadata_recorder.metadata_recorder import MetadataRecorder
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.submitter.processor_split import ProcessorSplit

# FIXME: You are here: This needs a clean-up
def test_metadata_recorder(get_bout_path_conduction, make_test_schema):
    db_connection, _ = make_test_schema('test_metadata_recorder')
    bout_paths = get_bout_path_conduction('test_metadata_recorder')

    metadata_recorder = MetadataRecorder(db_connection, bout_paths, RunParameters())

    metadata_recorder.capture_new_data_from_run(ProcessorSplit())

    # FIXME: Investigate the writing
    metadata_recorder.database_reader
