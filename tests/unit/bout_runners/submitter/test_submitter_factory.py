"""Contains unittests for the SubmitterFactory."""


import pytest
from bout_runners.submitter.submitter_factory import get_submitter
from bout_runners.submitter.local_submitter import LocalSubmitter


def test_submitter_factory() -> None:
    """Test that the SubmitterFactory returns Submitter objects."""
    submitter = get_submitter("local")
    assert isinstance(submitter, LocalSubmitter)

    with pytest.raises(NotImplementedError):
        get_submitter("not a class")
