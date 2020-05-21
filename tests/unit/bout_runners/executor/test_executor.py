"""Contains unittests for the executor."""


from bout_runners.executor.executor import Executor
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.submitter.local_submitter import LocalSubmitter


def test_executor(make_project, yield_bout_path_conduction):
    """
    Test that we are able to execute the conduction example.

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    yield_bout_path_conduction : function
        Function which makes the BoutPaths object for the conduction
        example
    """
    # Use the make fixture in order to automate clean up after done
    _ = make_project

    # Make the executor
    bout_paths = yield_bout_path_conduction("test_executor")
    run_parameters = RunParameters({"global": {"nout": 0}})
    executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
    )

    executor.execute()

    log_path = bout_paths.bout_inp_dst_dir.joinpath("BOUT.log.0")

    assert log_path.is_file()
