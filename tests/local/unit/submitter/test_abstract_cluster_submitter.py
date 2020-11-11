"""Contains unittests for the abstract cluster submitter."""


import pytest

from bout_runners.submitter.abstract_submitters import AbstractClusterSubmitter


def test_local_submitter() -> None:
    """Test that AbstractClusterSubmitter obtain time from string."""
    pbs_time = "65:43:21"
    (
        pbs_days,
        pbs_hours,
        pbs_minutes,
        pbs_seconds,
    ) = AbstractClusterSubmitter.get_days_hours_minutes_seconds_from_str(pbs_time)
    assert pbs_days == 0
    assert pbs_hours == 65
    assert pbs_minutes == 43
    assert pbs_seconds == 21

    slurm_time = "1-65:43:21"
    (
        slurm_days,
        slurm_hours,
        slurm_minutes,
        slurm_seconds,
    ) = AbstractClusterSubmitter.get_days_hours_minutes_seconds_from_str(slurm_time)
    assert slurm_days == 1
    assert slurm_hours == 65
    assert slurm_minutes == 43
    assert slurm_seconds == 21

    with pytest.raises(ValueError):
        AbstractClusterSubmitter.get_days_hours_minutes_seconds_from_str(
            "Not a time string"
        )
