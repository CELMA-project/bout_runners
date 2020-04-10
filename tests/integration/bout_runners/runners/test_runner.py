"""Contains integration test for the runner."""


from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.executor.executor import Executor
from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.runner.bout_runner import BoutRunner


def test_bout_runner(make_project,
                     yield_number_of_rows_for_all_tables):
    """
    Test that the BoutRunner can execute a run.

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a
        schema
    """
    name = 'test_bout_runner_integration'
    # Make project to save time
    project_path = make_project

    # Create the `bout_paths` object
    bout_inp_src_dir = project_path.joinpath('data')
    bout_inp_dst_dir = project_path.joinpath(name)
    bout_paths = BoutPaths(project_path=project_path,
                           bout_inp_src_dir=bout_inp_src_dir,
                           bout_inp_dst_dir=bout_inp_dst_dir)

    # Create the input objects
    run_parameters = RunParameters({'global': {'nout': 0}})
    default_parameters = DefaultParameters(bout_paths)
    final_parameters = FinalParameters(default_parameters)
    executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters)
    db_connection = DatabaseConnector(name)

    # Run the project
    runner = BoutRunner(executor,
                        db_connection,
                        final_parameters)
    runner.run()

    # Assert that the run went well
    db_reader = DatabaseReader(db_connection)
    assert bout_paths.bout_inp_dst_dir.joinpath('BOUT.dmp.0.nc').\
        is_file()
    assert db_reader.check_tables_created()
    # Assert that all the values are 1
    number_of_rows_dict = \
        yield_number_of_rows_for_all_tables(db_reader)
    assert sum(number_of_rows_dict.values()) == \
        len(number_of_rows_dict.keys())
