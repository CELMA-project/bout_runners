"""Contains PBS unit tests for the runner."""


from pathlib import Path
import pytest
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.submitter.pbs_submitter import PBSSubmitter
from tests.utils.run import assert_waiting_for_graph


def test_pure_pbs_graph(tmp_path: Path):
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

    assert_waiting_for_graph(
        node_zero_submitter,
        node_one_submitter,
        node_two_submitter,
        node_three_submitter,
        save_dir,
    )


@pytest.mark.timeout(60)
def test_mixed_pbs_graph(tmp_path: Path):
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

    assert_waiting_for_graph(
        node_zero_submitter,
        node_one_submitter,
        node_two_submitter,
        node_three_submitter,
        save_dir,
    )
