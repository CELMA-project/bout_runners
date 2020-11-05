"""Contains the test for the documentation."""


from typing import Callable, Tuple

from pathlib import Path
from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.executor.executor import Executor
from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.run_group import RunGroup
from bout_runners.runner.bout_runner import BoutRunner

from tests.utils.dummy_functions import return_none, mock_expand


def test_restart_documentation(
    clean_up_bout_inp_src_and_dst: Callable[[str, str], Tuple[Path, Path, Path]]
) -> None:
    """
    Test that the restart documentation runs without error.

    Parameters
    ----------
    clean_up_bout_inp_src_and_dst : function
        Function which adds temporary BOUT.inp directories to removal.
    """
    # NOTE: We are aware of the number of locals, and are here only testing the docs
    # pylint: disable=too-many-locals
    project_path, bout_inp_src_dir, bout_inp_dst_dir = clean_up_bout_inp_src_and_dst(
        "test_restart_documentation_src", "test_restart_documentation_dst"
    )
    bout_paths = BoutPaths(
        project_path=project_path,
        bout_inp_src_dir=bout_inp_src_dir,
        bout_inp_dst_dir=bout_inp_dst_dir,
    )

    default_parameters = DefaultParameters(bout_paths)
    run_parameters = RunParameters({"global": {"nout": 0}})
    final_parameters = FinalParameters(default_parameters, run_parameters)

    basic_executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
    )

    # NOTE: We set the database to bout_inp_dst_dir as this will be removed later
    db_connector = DatabaseConnector("name_of_database", db_root_path=bout_inp_dst_dir)

    basic_bout_run_setup = BoutRunSetup(basic_executor, db_connector, final_parameters)

    run_graph = RunGraph()
    name = "my_restart_runs"
    basic_run_group = RunGroup(run_graph, basic_bout_run_setup, name=name)

    # New section in the documentation

    restart_executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
        restart_from=bout_paths.bout_inp_dst_dir,
    )

    restart_bout_run_setup = BoutRunSetup(
        restart_executor, db_connector, final_parameters
    )

    RunGroup(
        run_graph,
        restart_bout_run_setup,
        name=name,
        waiting_for=basic_run_group.bout_run_node_name,
    )

    # New section in the documentation

    new_run_parameters = RunParameters({"solver": {"adams_moulton": True}})
    new_final_parameters = FinalParameters(default_parameters, run_parameters)

    restart_with_changing_parameters_executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=new_run_parameters,
        restart_from=bout_paths.bout_inp_dst_dir,
    )

    BoutRunSetup(
        restart_with_changing_parameters_executor, db_connector, new_final_parameters
    )

    RunGroup(
        run_graph,
        restart_bout_run_setup,
        name=name,
        waiting_for=basic_run_group.bout_run_node_name,
    )

    # New section in the documentation

    run_graph.get_dot_string()

    # New section in the documentation

    runner = BoutRunner(run_graph)
    runner.run()


def test_pre_and_post_documentation(
    clean_up_bout_inp_src_and_dst: Callable[[str, str], Tuple[Path, Path, Path]]
) -> None:
    """
    Test that the pre and post documentation runs without error.

    FIXME: Explain documentation how bout_runners copies restart files

    Parameters
    ----------
    clean_up_bout_inp_src_and_dst : function
        Function which adds temporary BOUT.inp directories to removal.
    """
    # NOTE: We are aware of the number of locals, and are here only testing the docs
    # pylint: disable=too-many-locals
    project_path, bout_inp_src_dir, bout_inp_dst_dir = clean_up_bout_inp_src_and_dst(
        "test_pre_post_documentation_src", "test_pre_post_documentation_dst"
    )

    bout_paths = BoutPaths(
        project_path=project_path,
        bout_inp_src_dir=bout_inp_src_dir,
        bout_inp_dst_dir=bout_inp_dst_dir,
    )

    default_parameters = DefaultParameters(bout_paths)
    run_parameters = RunParameters({"global": {"nout": 0}})
    final_parameters = FinalParameters(default_parameters, run_parameters)

    basic_executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
    )

    # NOTE: We set the database to bout_inp_dst_dir as this will be removed later
    db_connector = DatabaseConnector("name_of_database", db_root_path=bout_inp_dst_dir)

    basic_bout_run_setup = BoutRunSetup(basic_executor, db_connector, final_parameters)

    run_graph = RunGraph()
    name = "my_restart_runs"
    basic_run_group = RunGroup(run_graph, basic_bout_run_setup, name=name)

    # New section in the documentation

    basic_run_group.add_post_processor(
        {"function": return_none, "args": None, "kwargs": None}
    )
    expanded_noise_restarts_dir = bout_paths.bout_inp_dst_dir.parent.joinpath(
        "expanded_noise_restarts"
    )
    kwargs = {
        "newNz": 16,
        "path": bout_paths.bout_inp_dst_dir,
        "output": expanded_noise_restarts_dir,
    }
    expand_node_name = basic_run_group.add_post_processor(
        {
            "function": mock_expand,
            "args": None,
            "kwargs": kwargs,
        },
    )

    # New section in the documentation
    # NOTE: Add these for removal
    clean_up_bout_inp_src_and_dst("expanded_noise_restarts", "expanded_noise_restarts")

    # Create the RunGroup
    restart_executor = Executor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
        restart_from=expanded_noise_restarts_dir,
    )

    restart_bout_run_setup = BoutRunSetup(
        restart_executor, db_connector, final_parameters
    )

    restart_run_group = RunGroup(
        run_graph, restart_bout_run_setup, name=name, waiting_for=expand_node_name
    )

    kwargs = {"path": expanded_noise_restarts_dir, "scale": 1e-5}
    restart_run_group.add_pre_processor(
        {
            "function": return_none,
            "args": None,
            "kwargs": kwargs,
        },
        waiting_for=expand_node_name,
    )

    # New section in the documentation

    run_graph.get_dot_string()

    # New section in the documentation

    runner = BoutRunner(run_graph)
    runner.run()
