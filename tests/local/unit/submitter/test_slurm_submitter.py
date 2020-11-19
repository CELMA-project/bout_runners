"""Contains unittests for the local part of the SLURM submitter."""


from pathlib import Path
from typing import Dict, Optional

from bout_runners.submitter.abstract_cluster_submitter import AbstractClusterSubmitter
from bout_runners.submitter.abstract_submitter import AbstractSubmitter
from bout_runners.submitter.slurm_submitter import SLURMSubmitter


def test___init__() -> None:
    """Test that the SLURMSubmitter constructor gives correct type."""
    submitter = SLURMSubmitter("__init__test", Path())
    assert isinstance(submitter, AbstractSubmitter)
    assert isinstance(submitter, AbstractClusterSubmitter)
    assert isinstance(submitter, SLURMSubmitter)

    submitter = SLURMSubmitter(
        "test",
        Path(),
        {
            "walltime": "65:43:21",
            "mail": "joe@doe.com",
            "queue": "workq",
            "account": "common",
        },
    )
    assert isinstance(submitter, AbstractSubmitter)
    assert isinstance(submitter, AbstractClusterSubmitter)
    assert isinstance(submitter, SLURMSubmitter)


def test_structure_time_to_slurm_format() -> None:
    """Test that we can structure time to SLURM format."""
    result = SLURMSubmitter.structure_time_to_slurm_format("1-65:43:21")
    expected = "3-17:43:21"
    assert expected == result


def test_create_submission_string() -> None:
    """Test that the format of the submission string is correct."""
    job_name = "test_create_submission_string_small"
    submitter = SLURMSubmitter(job_name, Path())
    result = submitter.create_submission_string("ls", waiting_for=tuple())
    expected = (
        "#!/bin/bash\n"
        f"#SBATCH --job-name={job_name}\n"
        "#SBATCH --nodes=1\n"
        "#SBATCH --tasks-per-node=1\n"
        f"#SBATCH -o {job_name}.log\n"
        f"#SBATCH -e {job_name}.err\n"
        "\n"
        "cd $SLURM_SUBMIT_DIR\n"
        "ls"
    )
    assert result == expected

    job_name = "test_create_submission_string_full"
    submitter = SLURMSubmitter(
        job_name,
        Path(),
        {
            "walltime": "65:43:21",
            "mail": "joe@doe.com",
            "queue": "workq",
            "account": "common",
        },
    )
    result = submitter.create_submission_string("ls", waiting_for=tuple())
    expected = (
        "#!/bin/bash\n"
        f"#SBATCH --job-name={job_name}\n"
        "#SBATCH --nodes=1\n"
        "#SBATCH --tasks-per-node=1\n"
        "#SBATCH --time=2-17:43:21\n"
        "#SBATCH --account=common\n"
        "#SBATCH -p workq\n"
        f"#SBATCH -o {job_name}.log\n"
        f"#SBATCH -e {job_name}.err\n"
        "#SBATCH --mail-type=ALL\n"
        "#SBATCH --mail-user=joe@doe.com\n"
        "\n"
        "cd $SLURM_SUBMIT_DIR\n"
        "ls"
    )

    assert result == expected


def test_get_return_code(get_sacct_dict: Dict[Optional[str], str]) -> None:
    """
    Test that we can obtain the return code from the trace.

    Parameters
    ----------
    get_sacct_dict : dict
        Dict where the keys are states and values are sacct_lines
    """
    for state, lines in get_sacct_dict.items():
        return_code = SLURMSubmitter(
            f"test_get_return_code_{state}", Path()
        ).get_return_code(lines)
        if state is None:
            assert return_code is None
        elif state == "FAILED":
            assert return_code == 2
        else:
            assert return_code == 0


def test_get_state(get_sacct_dict: Dict[Optional[str], str]) -> None:
    """
    Test that we read the dequeue status from the trace.

    Parameters
    ----------
    get_sacct_dict : dict
        Dict where the keys are states and values are sacct_lines
    """
    for state, lines in get_sacct_dict.items():
        assert SLURMSubmitter("test_dequeue_success", Path()).get_state(lines) == state
