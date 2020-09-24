"""Contains unittests for the local submitter."""


# NOTE: subprocess can be vulnerable if shell=True
#       However, CalledProcessError has no known security vulnerabilities
from subprocess import CalledProcessError  # nosec

import pytest
from bout_runners.submitter.local_submitter import LocalSubmitter


@pytest.mark.timeout(60)
def test_local_submitter() -> None:
    """Test that LocalSubmitter can run a command and raise an error."""
    submitter = LocalSubmitter()
    submitter.submit_command("ls")
    submitter.wait_until_completed()

    submitter.errored()
    assert isinstance(submitter.pid, int)
    assert isinstance(submitter.return_code, int)
    assert isinstance(submitter.std_out, str)
    assert isinstance(submitter.std_err, str)

    with pytest.raises(FileNotFoundError):
        submitter.submit_command("not a real command")
        submitter.wait_until_completed()
        submitter.raise_error()

    with pytest.raises(CalledProcessError):
        submitter.submit_command("ls ThisPathDoesNotExist")
        submitter.wait_until_completed()
        submitter.raise_error()
