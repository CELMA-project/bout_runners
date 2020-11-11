"""Contains unittests for the PBS part of the PBS submitter."""

from pathlib import Path

import pytest

from bout_runners.submitter.pbs_submitter import PBSSubmitter


@pytest.mark.timeout(60)
def test_submit_command(tmp_path: Path) -> None:
    """
    Test that we can submit a command.

    Parameters
    ----------
    tmp_path : Path
        Temporary path (pytest fixture)
    """
    job_name = "PBS_submit_test"
    store_path = tmp_path.joinpath(job_name)
    store_path.mkdir()
    submitter = PBSSubmitter(job_name, store_path)
    submitter.submit_command("ls")
    submitter.wait_until_completed()

    assert store_path.joinpath(f"{job_name}.log").is_file()
    assert store_path.joinpath(f"{job_name}.err").is_file()
    assert store_path.joinpath(f"{job_name}.sh").is_file()
    assert not submitter.errored()
    assert submitter.return_code == 0

    submitter.submit_command("/bin/i_dont_exist")

    with pytest.raises(RuntimeError):
        submitter.wait_until_completed()
        assert submitter.return_code == 254
        assert submitter.errored(raise_error=True)


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
    job_name = "PBS_test_completed"
    store_path = tmp_path.joinpath(job_name)
    store_path.mkdir()
    submitter = PBSSubmitter(job_name, store_path)
    assert not submitter.completed()

    submitter.submit_command("/bin/sleep 1000")
    assert not submitter.completed()

    submitter.kill()
    # Give system time to shut down process
    submitter.wait_until_completed(raise_error=False)
    assert submitter.completed()


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
    job_name = "PBS_test_first_submitter"
    store_path = tmp_path.joinpath(job_name)
    store_path.mkdir()
    first_submitter = PBSSubmitter(job_name, store_path)
    first_submitter.submit_command("sleep 30")

    # Create second submitter, which waits for the first
    job_name = "PBS_test_second_submitter"
    store_path = tmp_path.joinpath(job_name)
    store_path.mkdir()
    second_submitter = PBSSubmitter(job_name, store_path)
    second_submitter.add_waiting_for(first_submitter.job_id)
    assert len(second_submitter.waiting_for) == 1
    # NOTE: Sleep time must be large as there are some overhead for checking
    #       whether a job is complete
    second_submitter.submit_command("sleep 10")

    # Assert that second submitter is not completed before the first
    first_submitter.wait_until_completed(raise_error=True)
    assert first_submitter.completed()
    assert not second_submitter.completed()

    # Assert that the second submitter completes
    second_submitter.wait_until_completed(raise_error=True)
    assert second_submitter.completed()
