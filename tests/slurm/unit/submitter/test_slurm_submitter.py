"""Contains unittests for the SLURM part of the SLURM submitter."""


from pathlib import Path

import pytest

from bout_runners.submitter.slurm_submitter import SLURMSubmitter
from tests.utils.submitters import (
    add_waiting_for_tester,
    completed_tester,
    submit_command_tester,
)


@pytest.mark.timeout(60)
def test_submit_command(tmp_path: Path) -> None:
    """
    Test that we can submit a command.

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    """
    job_name = "SLURM_submit_test"
    submitter_class = SLURMSubmitter
    submit_command_tester(tmp_path, job_name, submitter_class)


@pytest.mark.timeout(60)
def test_completed(tmp_path: Path) -> None:
    """
    Test the completed function.

    This will test the part of the function which is not tested by normal
    completion

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    """
    job_name = "SLURM_test_completed"
    submitter_class = SLURMSubmitter
    completed_tester(tmp_path, job_name, submitter_class)


@pytest.mark.timeout(60)
def test_add_waiting_for(tmp_path: Path) -> None:
    """
    Test the functionality which adds jobs to wait for.

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    """
    # Create first submitter
    job_name = "SLURM_test_first_submitter"
    submitter_class = SLURMSubmitter
    add_waiting_for_tester(tmp_path, job_name, submitter_class)
