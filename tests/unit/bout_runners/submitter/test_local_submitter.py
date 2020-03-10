"""Contains unittests for the local submitter."""


import pytest
import subprocess
from bout_runners.submitter.local_submitter import LocalSubmitter


def test_local_submitter():
    """Test that LocalSubmitter can run a command and raise an error."""
    submitter = LocalSubmitter()
    result = submitter.submit_command('ls')
    assert type(result) == subprocess.CompletedProcess

    with pytest.raises(FileNotFoundError):
        submitter.submit_command('not a real command')
