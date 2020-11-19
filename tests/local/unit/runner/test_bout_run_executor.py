"""Contains unittests for the executor."""


from pathlib import Path
from typing import Callable

from bout_runners.runner.bout_run_executor import BoutRunExecutor


def test_bout_run_executor(
    make_project: Path, get_executor: Callable[[str], BoutRunExecutor]
) -> None:
    """
    Test that we are able to execute the conduction example.

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    get_executor : function
        Function which returns an BoutRunExecutor based on the conduction directory
    """
    # Use the make fixture in order to automate clean up after done
    _ = make_project

    # Make the executor
    executor = get_executor("test_executor")

    executor.execute()
    executor.submitter.wait_until_completed()
    log_path = executor.bout_paths.bout_inp_dst_dir.joinpath("BOUT.log.0")
    assert log_path.is_file()

    executor.execute(restart=True)
    executor.submitter.wait_until_completed()
    log_path = executor.bout_paths.bout_inp_dst_dir.joinpath("BOUT.log.0")
    assert log_path.is_file()
