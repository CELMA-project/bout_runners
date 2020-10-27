"""Contains unittests for the SubmitterFactory."""


import pytest
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.submitter.submitter_factory import get_submitter


def test_submitter_factory() -> None:
    """Test that the SubmitterFactory returns Submitter objects."""
    submitter = get_submitter(name="local", argument_dict=dict())
    assert isinstance(submitter, LocalSubmitter)

    with pytest.raises(NotImplementedError):
        get_submitter(name="not a class", argument_dict=dict())
