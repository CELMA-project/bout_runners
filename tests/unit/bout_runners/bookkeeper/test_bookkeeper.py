"""Contains unittests for the bookkeeper."""


from bout_runners.bookkeeper.bookkeeper import Bookkeeper
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.submitter.processor_split import ProcessorSplit

# FIXME: You are here: This needs a clean-up
def test_bookkeeper(get_bout_path_conduction, make_test_schema):
    db_connection = make_test_schema('test_bookkeeper')
    bout_paths = get_bout_path_conduction('test_bookkeeper')

    bookkeeper = Bookkeeper(db_connection, bout_paths, RunParameters())

    bookkeeper.capture_new_data_from_run(ProcessorSplit())

    # FIXME: Investigate the writing
    bookkeeper.database_reader
