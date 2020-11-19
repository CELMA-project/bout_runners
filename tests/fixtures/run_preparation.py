"""Contains fixtures for preparation of runs."""


from typing import Callable, Optional

import pytest

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.parameters.bout_paths import BoutPaths
from bout_runners.parameters.bout_run_setup import BoutRunSetup
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.runner.bout_run_executor import BoutRunExecutor
from bout_runners.runner.run_graph import RunGraph


@pytest.fixture(scope="function")
def make_graph() -> RunGraph:
    """
    Yield a simple graph.

    Returns
    -------
    run_graph : RunGraph
        A simple graph
    """
    run_graph = RunGraph()
    for i in range(6):
        run_graph.add_function_node(str(i))

    run_graph.add_waiting_for("4", "3")
    run_graph.add_waiting_for("5", "3")
    run_graph.add_waiting_for("3", "2")
    run_graph.add_waiting_for("2", "0")
    run_graph.add_waiting_for("1", "0")
    return run_graph


@pytest.fixture(scope="function", name="get_executor")
def fixture_get_executor(
    yield_bout_path_conduction: Callable[[str], BoutPaths]
) -> Callable[[str], BoutRunExecutor]:
    """
    Return a function which returns an BoutRunExecutor object.

    Parameters
    ----------
    yield_bout_path_conduction : function
        Function which makes the BoutPaths object for the conduction example

    Returns
    -------
    _get_executor : function
        Function which returns an BoutRunExecutor based on the conduction directory
    """

    def _get_executor(tmp_path_name: str) -> BoutRunExecutor:
        """
        Create Executor based on the conduction directory.

        Parameters
        ----------
        tmp_path_name : str
            Name of the temporary directory

        Returns
        -------
        executor : BoutRunExecutor
            The Executor object
        """
        bout_paths = yield_bout_path_conduction(tmp_path_name)
        executor = DefaultParameters.get_test_executor(bout_paths)

        return executor

    return _get_executor


@pytest.fixture(scope="function")
def get_bout_run_setup(
    get_executor: Callable[[str], BoutRunExecutor],
    make_test_database: Callable[[Optional[str]], DatabaseConnector],
    get_default_parameters: DefaultParameters,
) -> Callable[[str], BoutRunSetup]:
    """
    Return a function which returns a BoutRunSetup object.

    Parameters
    ----------
    get_executor : function
        Function which returns an BoutRunExecutor based on the conduction directory
    make_test_database : function
        Function making an empty database
    get_default_parameters : DefaultParameters
        The DefaultParameters object

    Returns
    -------
    _get_bout_run_setup : function
        Function which returns the BoutRunSetup object based on the conduction directory
    """

    def _get_bout_run_setup(tmp_path_name: str) -> BoutRunSetup:
        """
        Create BoutRunSetup based on the conduction directory.

        Parameters
        ----------
        tmp_path_name : str
            Name of the temporary directory

        Returns
        -------
        bout_run_setup : BoutRunSetup
            The BoutRunSetup object
        """
        executor = get_executor(tmp_path_name)
        db_connector = make_test_database(tmp_path_name)
        final_parameters = FinalParameters(get_default_parameters)
        bout_run_setup = BoutRunSetup(executor, db_connector, final_parameters)

        return bout_run_setup

    return _get_bout_run_setup
