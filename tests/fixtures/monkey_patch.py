"""Contains monkey patches."""


from pathlib import Path
from typing import Callable, Optional

import psutil
import pytest
from _pytest.monkeypatch import MonkeyPatch


@pytest.fixture(scope="function")
def mock_pid_exists(monkeypatch: MonkeyPatch) -> Callable:
    """
    Return a function for setting up a monkeypatch of psutil.pid_exists.

    Parameters
    ----------
    monkeypatch : MonkeyPatch
        MonkeyPatch from pytest

    Returns
    -------
    mock_wrapper : function
        Function which returns a monkeypatch for psutil.pid_exists
    """

    def mock_wrapper(test_case: str):
        """
        Return monkeypatch for psutil.pid_exists.

        Note that this function wrap the mock function in order to set
        test_case

        Parameters
        ----------
        test_case : str
            Description of the test on the form

            >>> ('<log_file_present>_<pid_present_in_log>_'
            ...  '<started_time_present_in_log>_'
            ...  '<ended_time_present_in_log>_'
            ...  '<whether_pid_exists>_<new_status>')
        """

        def _pid_exists_mock(pid: Optional[int]) -> bool:
            """
            Mock psutil.pid_exists.

            Parameters
            ----------
            pid : int or None
                Processor id to check

            Returns
            -------
            bool
                Whether or not the pid exists (in a mocked form)
            """
            return (pid == 10 and "no_mock_pid" not in test_case) or pid == 11

        monkeypatch.setattr(psutil, "pid_exists", _pid_exists_mock)

    return mock_wrapper


# NOTE: MonkeyPatch is function scoped
@pytest.fixture(scope="function")
def get_mock_config_path(
    monkeypatch: MonkeyPatch, create_mock_config_path: Path
) -> Path:
    """
    Return a mock path for the config dir and redirects get_config_path.

    Parameters
    ----------
    monkeypatch : MonkeyPatch
        MonkeyPatch from pytest
    create_mock_config_path : Path
        Path to a mock config

    Returns
    -------
    mock_config_path : Path
        The mocked config directory
    """
    mock_config_path = create_mock_config_path

    # Redirect reading of config_files to mock_config_path for these
    # tests
    monkeypatch.setattr(
        "bout_runners.utils.paths.get_config_path", lambda: mock_config_path
    )

    return mock_config_path
