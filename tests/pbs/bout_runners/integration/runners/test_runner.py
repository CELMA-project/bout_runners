"""Contains PBS integration tests for the runner."""


from pathlib import Path
import pytest
from tests.utils.cluster_node_functions import node_zero, node_one, node_two, node_three
from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.bout_runner import BoutRunner
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.submitter.pbs_submitter import PBSSubmitter


def test_pure_pbs_runner(tmp_path: Path):
    """
    Test dependency with several PBS nodes.

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    """
    job_name = "PBS_test_pure_pbs_runner"
    save_dir = tmp_path.joinpath(job_name)
    save_dir.mkdir()
    node_zero_submitter = PBSSubmitter("node_zero", save_dir)
    node_one_submitter = PBSSubmitter("node_one", save_dir)
    node_two_submitter = PBSSubmitter("node_two", save_dir)
    node_three_submitter = PBSSubmitter("node_three", save_dir)

    graph = RunGraph()
    graph.add_function_node(
        "node_zero",
        {"function": node_zero, "args": (save_dir,), "kwargs": None},
        save_dir.joinpath("node_zero.py"),
        node_zero_submitter,
    )
    graph.add_function_node(
        "node_one",
        {"function": node_one, "args": (save_dir,), "kwargs": None},
        save_dir.joinpath("node_one.py"),
        node_one_submitter,
    )
    graph.add_function_node(
        "node_two",
        {"function": node_two, "args": (save_dir,), "kwargs": None},
        save_dir.joinpath("node_two.py"),
        node_two_submitter,
    )
    graph.add_function_node(
        "node_three",
        {"function": node_three, "args": (save_dir,), "kwargs": None},
        save_dir.joinpath("node_three.py"),
        node_three_submitter,
    )

    graph.add_waiting_for("node_two", "node_one")
    graph.add_waiting_for("node_three", ("node_one", "node_two"))

    runner = BoutRunner(graph)
    runner.run()

    node_three_submitter.wait_until_completed()
    assert save_dir.joinpath("node_three.log").is_file()


@pytest.mark.timeout(60)
def test_mixed_pbs_runner(tmp_path: Path):
    """
    Test dependency with several PBS and local nodes.

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    """
    job_name = "PBS_test_mixed_pbs_runner"
    save_dir = tmp_path.joinpath(job_name)
    save_dir.mkdir()
    node_zero_submitter = PBSSubmitter("node_zero", save_dir)
    node_one_submitter = PBSSubmitter("node_one", save_dir)
    node_two_submitter = LocalSubmitter(save_dir)
    node_three_submitter = PBSSubmitter("node_three", save_dir)

    graph = RunGraph()
    graph.add_function_node(
        "node_zero",
        {"function": node_zero, "args": (save_dir,), "kwargs": None},
        save_dir.joinpath("node_zero.py"),
        node_zero_submitter,
    )
    graph.add_function_node(
        "node_one",
        {"function": node_one, "args": (save_dir,), "kwargs": None},
        save_dir.joinpath("node_one.py"),
        node_one_submitter,
    )
    graph.add_function_node(
        "node_two",
        {"function": node_two, "args": (save_dir,), "kwargs": None},
        save_dir.joinpath("node_two.py"),
        node_two_submitter,
    )
    graph.add_function_node(
        "node_three",
        {"function": node_three, "args": (save_dir,), "kwargs": None},
        save_dir.joinpath("node_three.py"),
        node_three_submitter,
    )

    graph.add_waiting_for("node_two", "node_one")
    graph.add_waiting_for("node_three", ("node_one", "node_two"))

    runner = BoutRunner(graph)
    runner.run()

    node_three_submitter.wait_until_completed()
    assert save_dir.joinpath("node_three.log").is_file()
