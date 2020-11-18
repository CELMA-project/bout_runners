"""Contains SLURM unit tests for the runner."""


from pathlib import Path

import pytest

from bout_runners.submitter.slurm_submitter import SLURMSubmitter
from tests.utils.submitters import submitter_graph_tester


@pytest.mark.timeout(60)
def test_pure_slurm_graph(tmp_path: Path):
    """
    Test dependency with several SLURM nodes.

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    """
    job_name = "SLURM_test_pure_slurm_runner"
    submitter_class = SLURMSubmitter
    submitter_graph_tester(tmp_path, job_name, submitter_class, local_node_two=False)


@pytest.mark.timeout(60)
def test_mixed_slurm_graph(tmp_path: Path):
    """
    Test dependency with several SLURM and local nodes.

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    """
    job_name = "SLURM_test_mixed_slurm_runner"
    submitter_class = SLURMSubmitter
    submitter_graph_tester(tmp_path, job_name, submitter_class, local_node_two=True)
