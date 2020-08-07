"""Contains unittests for the BOUT++ run setup."""


from typing import Callable
from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.executor.executor import Executor
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters


def test_bout_run_setup(
    get_executor: Callable,
    make_test_database: Callable,
    get_default_parameters: DefaultParameters,
) -> None:
    """
    Test the bout run setup.

    # FIXME: You are here: Make this a fixture, and test RunGroup

    Parameters
    ----------
    get_executor : function
        Function which returns an Executor based on the conduction directory
    make_test_database : function
        Function making an empty database
    get_default_parameters : DefaultParameters
        The DefaultParameters object
    """
    name = "test_bout_run_setup"
    executor = get_executor(name)
    db_connector = make_test_database(name)
    final_parameters = FinalParameters(get_default_parameters)
    bout_run_setup = BoutRunSetup(executor, db_connector, final_parameters)

    assert isinstance(bout_run_setup.executor, Executor)
    assert isinstance(bout_run_setup.final_parameters, FinalParameters)
    assert isinstance(bout_run_setup.db_connector, DatabaseConnector)
