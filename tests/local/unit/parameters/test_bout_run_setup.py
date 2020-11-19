"""Contains unittests for the BOUT++ run setup."""


from typing import Callable

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.parameters.bout_run_setup import BoutRunSetup
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.runner.bout_run_executor import BoutRunExecutor


def test_bout_run_setup(get_bout_run_setup: Callable[[str], BoutRunSetup]) -> None:
    """
    Test the bout run setup.

    Parameters
    ----------
    get_bout_run_setup : function
        Function which returns the BoutRunSetup object based on the conduction directory
    """
    bout_run_setup = get_bout_run_setup("test_bout_run_setup")

    assert isinstance(bout_run_setup.executor, BoutRunExecutor)
    assert isinstance(bout_run_setup.final_parameters, FinalParameters)
    assert isinstance(bout_run_setup.db_connector, DatabaseConnector)
