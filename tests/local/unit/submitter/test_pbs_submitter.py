"""Contains unittests for the local part of the PBS submitter."""


from pathlib import Path
from bout_runners.submitter.pbs_submitter import AbstractSubmitter
from bout_runners.submitter.pbs_submitter import AbstractClusterSubmitter
from bout_runners.submitter.pbs_submitter import PBSSubmitter


def test___init__() -> None:
    """Test that the PBSSubmitter constructor gives correct type."""
    submitter = PBSSubmitter("__init__test", Path())
    assert isinstance(submitter, AbstractSubmitter)
    assert isinstance(submitter, AbstractClusterSubmitter)
    assert isinstance(submitter, PBSSubmitter)

    submitter = PBSSubmitter(
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
    assert isinstance(submitter, PBSSubmitter)


def test_structure_time_to_pbs_format() -> None:
    """Test that we can structure time to PBS format."""
    result = PBSSubmitter.structure_time_to_pbs_format("1-65:43:21")
    expected = "89:43:21"
    assert expected == result


def test_create_submission_string() -> None:
    """Test that the format of the submission string is correct."""
    job_name = "test_create_submission_string_small"
    submitter = PBSSubmitter(job_name, Path())
    result = submitter.create_submission_string("ls", waiting_for=tuple())
    expected = (
        "#!/bin/bash\n"
        f"#PBS -N {job_name}\n"
        "#PBS -l nodes=1:ppn=1\n"
        f"#PBS -o {job_name}.log\n"
        f"#PBS -e {job_name}.err\n"
        "\n"
        "cd $PBS_O_WORKDIR\n"
        "ls"
    )
    assert result == expected

    job_name = "test_create_submission_string_full"
    submitter = PBSSubmitter(
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
        f"#PBS -N {job_name}\n"
        "#PBS -l nodes=1:ppn=1\n"
        "#PBS -l walltime=65:43:21\n"
        "#PBS -A common\n"
        "#PBS -q workq\n"
        f"#PBS -o {job_name}.log\n"
        f"#PBS -e {job_name}.err\n"
        "#PBS -m abe\n"
        "#PBS -M joe@doe.com\n"
        "\n"
        "cd $PBS_O_WORKDIR\n"
        "ls"
    )

    assert result == expected


def test_get_return_code(get_test_data_path: Path) -> None:
    """
    Test that we can obtain the return code from the trace.

    Parameters
    ----------
    get_test_data_path : path
        Path to the test data
    """
    trace_path = get_test_data_path.joinpath("test_trace")
    with trace_path.open("r") as file:
        trace = file.read()

    return_code = PBSSubmitter("test_get_return_code_success", Path()).get_return_code(
        trace
    )
    assert return_code == 0

    trace = trace.replace("Exit_status=0", "Exit_status=255")

    return_code = PBSSubmitter("test_get_return_code_fail", Path()).get_return_code(
        trace
    )
    assert return_code == 255


def test_has_dequeue(get_test_data_path: Path) -> None:
    """
    Test that we read the dequeue status from the trace.

    Parameters
    ----------
    get_test_data_path : path
        Path to the test data
    """
    trace_path = get_test_data_path.joinpath("test_trace")
    with trace_path.open("r") as file:
        trace = file.read()

    assert PBSSubmitter("test_dequeue_success", Path()).has_dequeue(trace)

    trace = "\n".join(trace.split("\n")[:3])

    assert not PBSSubmitter("test_dequeue_fail", Path()).has_dequeue(trace)
