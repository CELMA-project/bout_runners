"""Contains the test for the documentation."""


from pathlib import Path
from typing import Callable

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.parameters.bout_paths import BoutPaths
from bout_runners.parameters.bout_run_setup import BoutRunSetup
from bout_runners.parameters.default_parameters import DefaultParameters
from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.parameters.run_parameters import RunParameters
from bout_runners.runner.bout_run_executor import BoutRunExecutor
from bout_runners.runner.bout_runner import BoutRunner
from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.run_group import RunGroup
from bout_runners.submitter.local_submitter import LocalSubmitter
from tests.utils.dummy_functions import mock_expand, return_none
from tests.utils.paths import FileStateRestorer


def test_restart_documentation(
    make_project: Path,
    copy_bout_inp: Callable[[Path, str], Path],
    file_state_restorer: FileStateRestorer,
) -> None:
    """
    Test that the restart documentation runs without error.

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    copy_bout_inp : function
        Function which copies BOUT.inp and returns the path to the temporary
        directory
    file_state_restorer : FileStateRestorer
        Object for restoring files to original state
    """
    # NOTE: We are aware of the number of locals, and are here only testing the docs
    # pylint: disable=too-many-locals
    project_path = make_project
    bout_inp_src_dir = copy_bout_inp(project_path, "test_restart_documentation_src")
    bout_inp_dst_dir = project_path.joinpath("test_restart_documentation_dst")
    # NOTE: bout_inp_src_dir removed by copy_bout_inp teardown
    file_state_restorer.add(bout_inp_dst_dir, force_mark_removal=True)

    bout_paths = BoutPaths(
        project_path=project_path,
        bout_inp_src_dir=bout_inp_src_dir,
        bout_inp_dst_dir=bout_inp_dst_dir,
    )

    default_parameters = DefaultParameters(bout_paths)
    run_parameters = RunParameters({"global": {"nout": 0}})
    final_parameters = FinalParameters(default_parameters, run_parameters)

    basic_executor = BoutRunExecutor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
    )

    # NOTE: We set the database to bout_inp_dst_dir as this will be removed later
    db_connector = DatabaseConnector("name_of_database", db_root_path=bout_inp_dst_dir)
    file_state_restorer.add(db_connector.db_path, force_mark_removal=True)

    basic_bout_run_setup = BoutRunSetup(basic_executor, db_connector, final_parameters)

    run_graph = RunGraph()
    name = "my_restart_runs"
    basic_run_group = RunGroup(run_graph, basic_bout_run_setup, name=name)

    # New section in the documentation

    restart_executor = BoutRunExecutor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
        restart_from=bout_paths.bout_inp_dst_dir,
    )
    file_state_restorer.add(
        restart_executor.bout_paths.bout_inp_dst_dir, force_mark_removal=True
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

    restart_with_changing_parameters_executor = BoutRunExecutor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=new_run_parameters,
        restart_from=bout_paths.bout_inp_dst_dir,
    )
    file_state_restorer.add(
        restart_with_changing_parameters_executor.bout_paths.bout_inp_dst_dir,
        force_mark_removal=True,
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
    make_project: Path,
    copy_bout_inp: Callable[[Path, str], Path],
    file_state_restorer: FileStateRestorer,
) -> None:
    """
    Test that the pre and post documentation runs without error.

    Parameters
    ----------
    make_project : Path
        The path to the conduction example
    copy_bout_inp : function
        Function which copies BOUT.inp and returns the path to the temporary
        directory
    file_state_restorer : FileStateRestorer
        Object for restoring files to original state
    """
    # NOTE: We are aware of the number of locals, and are here only testing the docs
    # pylint: disable=too-many-locals
    project_path = make_project
    bout_inp_src_dir = copy_bout_inp(project_path, "test_pre_post_documentation_src")
    bout_inp_dst_dir = project_path.joinpath("test_pre_post_documentation_dst")
    # NOTE: bout_inp_src_dir removed by copy_bout_inp teardown
    file_state_restorer.add(bout_inp_dst_dir, force_mark_removal=True)

    bout_paths = BoutPaths(
        project_path=project_path,
        bout_inp_src_dir=bout_inp_src_dir,
        bout_inp_dst_dir=bout_inp_dst_dir,
    )

    default_parameters = DefaultParameters(bout_paths)
    run_parameters = RunParameters({"global": {"nout": 0}})
    final_parameters = FinalParameters(default_parameters, run_parameters)

    basic_executor = BoutRunExecutor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
    )

    # NOTE: We set the database to bout_inp_dst_dir as this will be removed later
    db_connector = DatabaseConnector("name_of_database", db_root_path=bout_inp_dst_dir)
    file_state_restorer.add(db_connector.db_path, force_mark_removal=True)

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
    file_state_restorer.add(expanded_noise_restarts_dir, force_mark_removal=True)
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

    # Create the RunGroup
    restart_executor = BoutRunExecutor(
        bout_paths=bout_paths,
        submitter=LocalSubmitter(bout_paths.project_path),
        run_parameters=run_parameters,
        restart_from=expanded_noise_restarts_dir,
    )
    file_state_restorer.add(
        restart_executor.bout_paths.bout_inp_dst_dir, force_mark_removal=True
    )

    restart_bout_run_setup = BoutRunSetup(
        restart_executor, db_connector, final_parameters
    )

    restart_run_group = RunGroup(run_graph, restart_bout_run_setup, name=name)

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
