"""Contains unittests for the local submitter."""


import subprocess
import pytest
from bout_runners.submitter.local_submitter import LocalSubmitter


def test_local_submitter():
    """Test that LocalSubmitter can run a command and raise an error."""
    submitter = LocalSubmitter()
    result = submitter.submit_command('ls')
    assert isinstance(result, subprocess.CompletedProcess)
    assert isinstance(submitter.pid, int)

    with pytest.raises(FileNotFoundError):
        submitter.submit_command('not a real command')
