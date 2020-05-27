"""Contains unittests for the local submitter."""


# NOTE: subprocess can be vulnerable if shell=True
#       However, CompletedProcess has no known security vulnerabilities
from subprocess import CompletedProcess  # nosec

import pytest
from bout_runners.submitter.local_submitter import LocalSubmitter


def test_local_submitter() -> None:
    """Test that LocalSubmitter can run a command and raise an error."""
    submitter = LocalSubmitter()
    result = submitter.submit_command("ls")
    assert isinstance(result, CompletedProcess)
    assert isinstance(submitter.pid, int)

    with pytest.raises(FileNotFoundError):
        submitter.submit_command("not a real command")
