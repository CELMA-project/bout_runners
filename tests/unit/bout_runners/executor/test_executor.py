"""Contains unittests for the executor."""


from pathlib import Path
from typing import Callable

from bout_runners.parameters.default_parameters import DefaultParameters


def test_executor(make_project: Path, yield_bout_path_conduction: Callable) -> None:
    """
    Test that we are able to execute the conduction example.

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    yield_bout_path_conduction : function
        Function which makes the BoutPaths object for the conduction example
    """
    # Use the make fixture in order to automate clean up after done
    _ = make_project

    # Make the executor
    bout_paths = yield_bout_path_conduction("test_executor")
    executor = DefaultParameters.get_test_executor(bout_paths)

    executor.execute()

    log_path = bout_paths.bout_inp_dst_dir.joinpath("BOUT.log.0")

    assert log_path.is_file()
