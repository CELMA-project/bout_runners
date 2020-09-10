"""Contains functions for checking runs."""

from pathlib import Path
from typing import Callable, Dict, Optional, Union, Iterable

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.executor.executor import Executor
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.run_group import RunGroup
from bout_runners.submitter.local_submitter import LocalSubmitter


def assert_first_run(
    bout_paths: BoutPaths, db_connection: DatabaseConnector
) -> DatabaseReader:
    """
    Assert that the first run went well.

    Parameters
    ----------
    bout_paths : BoutPaths
        The object containing the paths
    db_connection : DatabaseConnector
        The database connection

    Returns
    -------
    db_reader : DatabaseReader
        The database reader object
    """
    db_reader = DatabaseReader(db_connection)
    assert_dump_files_exist(bout_paths.bout_inp_dst_dir)
    assert db_reader.check_tables_created()
    return db_reader


def assert_dump_files_exist(dump_dir: Path) -> None:
    """
    Assert that the dump files exits.

    Parameters
    ----------
    dump_dir : Path
        Path to the directory where the dump files of the project run is stored
    """
    assert (
        dump_dir.joinpath("BOUT.dmp.0.nc").is_file()
        or dump_dir.joinpath("BOUT.dmp.0.h5").is_file()
    )


def assert_tables_have_expected_len(
    db_reader: DatabaseReader,
    yield_number_of_rows_for_all_tables: Callable[[DatabaseReader], Dict[str, int]],
    expected_run_number: int,
    restarted: bool = False,
) -> None:
    """
    Assert that tables are of expected length.

    Parameters
    ----------
    db_reader : DatabaseReader
        The database reader object
    yield_number_of_rows_for_all_tables : function
        Function which returns the number of rows for all tables in a
        schema
    expected_run_number : int
        Expected number of runs to find
    restarted : bool
        Whether or not the run has been restarted
    """
    number_of_rows_dict = yield_number_of_rows_for_all_tables(db_reader)
    special_tables_count = dict()
    tables_changed_by_run = ("run",)
    # NOTE: When restarting, global.restart will change, which means that global_id
    #       in parameters will change
    #       The restart table however, will not change as the parameters therein only
    #       describes how the restart files are written
    tables_changed_by_restart = ("global", "parameters")
    for table_name in tables_changed_by_run + tables_changed_by_restart:
        special_tables_count[table_name] = number_of_rows_dict.pop(table_name)

    # Assert that all the runs are the same (with exception of run and restart)
    assert sum(number_of_rows_dict.values()) == len(number_of_rows_dict.keys())

    # Assert that the number of runs are correct
    assert special_tables_count["run"] == expected_run_number

    # Assert that the restart counter is correct
    if not restarted:
        expected_count = 1
    else:
        expected_count = 2
    for table in tables_changed_by_restart:
        assert special_tables_count[table] == expected_count


def make_run_group(
    name: str,
    make_project: Path,
    run_graph: Optional[RunGraph] = None,
    restart_from: Optional[Path] = None,
    waiting_for: Optional[Union[str, Iterable[str]]] = None,
) -> RunGroup:
    """
    Return a basic RunGroup.

    Parameters
    ----------
    run_graph
    name : str
        Name of RunGroup and DatabaseConnector
    make_project : Path
        The path to the conduction example
    run_graph : RunGraph
        The RunGraph object
    restart_from : Path or None
        The path to copy the restart files from
    waiting_for : None or str or iterable
        Name of nodes this node will wait for to finish before executing

    Returns
    -------
    run_group : RunGroup
        A basic run group
    """
    # Make project to save time
    project_path = make_project
    # Create the `bout_paths` object
    bout_paths = BoutPaths(
        project_path=project_path,
        bout_inp_src_dir=project_path.joinpath("data"),
        bout_inp_dst_dir=project_path.joinpath(name),
    )
    # Create the input objects
    run_parameters = RunParameters({"global": {"nout": 0}})
    default_parameters = DefaultParameters(bout_paths)
    final_parameters = FinalParameters(default_parameters, run_parameters)
    executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
        restart_from=restart_from,
    )
    db_connection = DatabaseConnector(name)
    bout_run_setup = BoutRunSetup(executor, db_connection, final_parameters)
    # Create the `run_group`
    run_graph = run_graph if run_graph is not None else RunGraph()
    run_group = RunGroup(run_graph, bout_run_setup, name=name, waiting_for=waiting_for)
    return run_group
