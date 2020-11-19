"""Contains functions for checking submitters."""


from pathlib import Path
from typing import Type

import pytest

from bout_runners.submitter.abstract_cluster_submitter import AbstractClusterSubmitter
from bout_runners.submitter.abstract_submitter import AbstractSubmitter
from bout_runners.submitter.local_submitter import LocalSubmitter
from tests.utils.run import assert_waiting_for_graph


def submit_command_tester(
    tmp_path: Path, job_name: str, submitter_class: Type[AbstractClusterSubmitter]
) -> None:
    """
    Test that the class can submit a command.

    Parameters
    ----------
    tmp_path: Path
        Temporary path (pytest fixture)
    job_name: str
        Name of the job
    submitter_class: Type[AbstractClusterSubmitter]
        The submitter to use
    """
    store_path = tmp_path.joinpath(job_name)
    store_path.mkdir()

    submitter = submitter_class(job_name, store_path)
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


def completed_tester(
    tmp_path: Path, job_name: str, submitter_class: Type[AbstractClusterSubmitter]
) -> None:
    """
    Test the completed function of the class.

    Parameters
    ----------
    tmp_path: Path
        Temporary path (pytest fixture)
    job_name: str
        Name of the job
    submitter_class: Type[AbstractClusterSubmitter]
        The submitter to use
    """
    store_path = tmp_path.joinpath(job_name)
    store_path.mkdir()
    submitter = submitter_class(job_name, store_path)
    assert not submitter.completed()
    submitter.submit_command("/bin/sleep 1000")
    assert not submitter.completed()
    submitter.kill()

    # Give system time to shut down process
    submitter.wait_until_completed(raise_error=False)
    assert submitter.completed()


def add_waiting_for_tester(
    tmp_path: Path, job_name: str, submitter_class: Type[AbstractClusterSubmitter]
) -> None:
    """
    Test the add_waiting_for function of the class.

    Parameters
    ----------
    tmp_path: Path
        Temporary path (pytest fixture)
    job_name: str
        Name of the job
    submitter_class: Type[AbstractClusterSubmitter]
        The submitter to use
    """
    store_path = tmp_path.joinpath(job_name)
    store_path.mkdir()
    first_submitter = submitter_class(job_name, store_path)
    first_submitter.submit_command("sleep 30")

    # Create second submitter, which waits for the first
    job_name = "PBS_test_second_submitter"
    store_path = tmp_path.joinpath(job_name)
    store_path.mkdir()
    second_submitter = submitter_class(job_name, store_path)
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


def submitter_graph_tester(
    tmp_path: Path,
    job_name: str,
    submitter_class: Type[AbstractClusterSubmitter],
    local_node_two: bool,
):
    """
    Test that the class can submit a command.

    Parameters
    ----------
    tmp_path: Path
        Temporary path (pytest fixture)
    job_name: str
        Name of the job
    submitter_class: Type[AbstractClusterSubmitter]
        The submitter to use
    local_node_two : bool
        Whether or not a local submitter should be used for node two
    """
    save_dir = tmp_path.joinpath(job_name)
    save_dir.mkdir()
    node_zero_submitter = submitter_class("node_zero", save_dir)
    node_one_submitter = submitter_class("node_one", save_dir)

    if local_node_two:
        node_two_submitter: AbstractSubmitter = LocalSubmitter(save_dir)
    else:
        node_two_submitter = submitter_class("node_two", save_dir)
    node_three_submitter = submitter_class("node_three", save_dir)

    assert_waiting_for_graph(
        node_zero_submitter,
        node_one_submitter,
        node_two_submitter,
        node_three_submitter,
        save_dir,
    )
