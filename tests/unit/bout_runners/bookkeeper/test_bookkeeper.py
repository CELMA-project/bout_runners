"""Contains unittests for the bookkeeper."""


from bout_runners.bookkeeper.bookkeeper import Bookkeeper


# FIXME: You are here: This needs a clean-up
def test_bookkeeper(make_test_schema):
    db_connection = make_test_schema('test_bookkeeper')

    bookkeeper = Bookkeeper(db_connection, bout_paths, run_parameters)

    bookkeeper.capture_new_data_from_run(processor_split)

    # FIXME: Investigate the writing
    bookkeeper.database_reader
