"""Contains unittests for the PBS part of the PBS submitter."""

import pytest
from pathlib import Path
from bout_runners.submitter.pbs_submitter import PBSSubmitter


@pytest.mark.timeout(60)
def test_submit_command() -> None:
    """
    Test that we can submit a command.

    # FIXME: You are here
    # FIXME: clean the shell-script when done
    # FIXME: assert fail due to
    https://community.openpbs.org/t/submit-job-but-command-not-found/1519/3
    """
    store_path = Path().home()
    job_name = "PBS_submit_test"
    submitter = PBSSubmitter(job_name, store_path)
    submitter.submit_command("ls")
    submitter.wait_until_completed()

    assert store_path.joinpath(f"{job_name}.log").is_file()
    assert store_path.joinpath(f"{job_name}.err").is_file()
