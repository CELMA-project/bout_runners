"""Contains unittests for the runner."""


from bout_runners.database.database_reader import DatabaseReader
from bout_runners.executor.executor import Executor
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.runner.bout_runner import BoutRunner


# FIXME: Make status object which updates the status
def test_bout_runner(yield_bout_path_conduction,
                     make_project,
                     make_test_schema,
                     yield_number_of_rows_for_all_tables):
    """
    Test that the BoutRunner can execute a run.

    Parameters
    ----------
    yield_bout_path_conduction : function
        Function which makes the BoutPaths object for the conduction
        example
    make_project : Path
        The path to the conduction example
    make_test_schema : function
        The function making the schema (i.e. making all the tables)
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a
        schema
    """
    # Making the project through the fixture
    _ = make_project
    db_connection, _ = make_test_schema('test_bout_runner')
    db_reader = DatabaseReader(db_connection)
    bout_paths = yield_bout_path_conduction('test_bout_runner')
    run_parameters = RunParameters({'global': {'nout': 0}})
    executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters)
    default_parameters = DefaultParameters(bout_paths)
    final_parameters = FinalParameters(default_parameters)

    runner = BoutRunner(executor,
                        db_connection,
                        final_parameters)
    runner.run()
    assert bout_paths.bout_inp_dst_dir.joinpath('BOUT.dmp.0.nc').\
        is_file()
    assert db_reader.check_tables_created()
    # Assert that all the values are 1
    number_of_rows_dict = \
        yield_number_of_rows_for_all_tables(db_reader)
    assert sum(number_of_rows_dict.values()) == \
        len(number_of_rows_dict.keys())
