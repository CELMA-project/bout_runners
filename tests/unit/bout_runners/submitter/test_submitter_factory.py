"""Contains unittests for the SubmitterFactory."""


import pytest
from bout_runners.submitter.submitter_factory import SubmitterFactory
from bout_runners.submitter.submitter_factory import LocalSubmitter


def test_submitter_factory():
    """Test that the SubmitterFactory returns Submitter objects."""
    submitter = SubmitterFactory.get_submitter('local')
    assert type(submitter) == LocalSubmitter

    with pytest.raises(NotImplementedError):
        SubmitterFactory.get_submitter('not a class')
