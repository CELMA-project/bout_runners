"""Contains fixtures for logs."""


import shutil
from pathlib import Path
from typing import Callable, Dict, Iterator

import pytest


@pytest.fixture(scope="session", name="yield_logs")
def fixture_yield_logs(get_test_data_path: Path) -> Iterator[Dict[str, Path]]:
    """
    Yield the different types of execution logs.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data

    Yields
    ------
    log_paths : dict of Path
        A dictionary containing the log paths used for testing
    """
    log_paths = dict()
    log_paths["success_log"] = get_test_data_path.joinpath("BOUT.log.0")
    log_paths["fail_log"] = get_test_data_path.joinpath("BOUT.log.0.fail")
    log_paths["unfinished_no_pid_log"] = get_test_data_path.joinpath(
        "BOUT.log.0.unfinished_no_pid"
    )
    log_paths["unfinished_not_started_log"] = get_test_data_path.joinpath(
        "BOUT.log.0.unfinished_not_started"
    )
    log_paths["unfinished_started_log"] = get_test_data_path.joinpath(
        "BOUT.log.0.unfinished_started"
    )
    log_paths["unfinished_started_log_pid_11"] = get_test_data_path.joinpath(
        "BOUT.log.0.unfinished_started_pid_11"
    )

    with Path(log_paths["success_log"]).open("r") as log_file:
        # Read only the first couple of lines
        all_lines = log_file.readlines()
        unfinished_no_pid_log = "".join(all_lines[:5])
        unfinished_not_started_log = "".join(all_lines[:100])
        unfinished_started_log = "".join(all_lines[:200])
        with log_paths["unfinished_no_pid_log"].open("w") as unfinished_file:
            unfinished_file.write(unfinished_no_pid_log)
        with log_paths["unfinished_not_started_log"].open("w") as unfinished_file:
            unfinished_not_started_log = unfinished_not_started_log.replace(
                "pid: 1191", "pid: 10"
            )
            unfinished_file.write(unfinished_not_started_log)
        with log_paths["unfinished_started_log"].open("w") as unfinished_file:
            unfinished_started_log = unfinished_started_log.replace(
                "pid: 1191", "pid: 10"
            )
            unfinished_file.write(unfinished_started_log)
        with log_paths["unfinished_started_log_pid_11"].open("w") as unfinished_file:
            unfinished_started_log = unfinished_started_log.replace(
                "pid: 10", "pid: 11"
            )
            unfinished_file.write(unfinished_started_log)

    yield log_paths

    # Clean-up
    log_paths["unfinished_no_pid_log"].unlink()
    log_paths["unfinished_not_started_log"].unlink()
    log_paths["unfinished_started_log"].unlink()
    log_paths["unfinished_started_log_pid_11"].unlink()


@pytest.fixture(scope="function", name="copy_log_file")
def fixture_copy_log_file(get_test_data_path: Path) -> Iterator[Callable]:
    """
    Return a function which copy log files to a temporary directory.

    Parameters
    ----------
    get_test_data_path : Path
        Path to test files

    Yields
    ------
    _copy_log_file : function
        Function which copy log files to a temporary directory
    """
    # NOTE: This corresponds to names in test.db
    paths_to_remove = list()

    def _copy_log_file(log_file_to_copy: Path, destination_dir_name: str) -> None:
        """
        Copy log files to a temporary directory.

        Parameters
        ----------
        log_file_to_copy : Path
            Path to log file to copy
        destination_dir_name : str
            Name of directory to copy relative to the test data dir
        """
        destination_dir = get_test_data_path.joinpath(destination_dir_name)
        destination_dir.mkdir(exist_ok=True)
        destination_path = destination_dir.joinpath("BOUT.log.0")
        shutil.copy(get_test_data_path.joinpath(log_file_to_copy), destination_path)
        paths_to_remove.append(destination_dir)

    yield _copy_log_file

    for path in paths_to_remove:
        shutil.rmtree(path)


@pytest.fixture(scope="function")
def copy_test_case_log_file(
    copy_log_file: Callable,
    get_test_data_path: Path,
    yield_logs: Dict[str, Path],
) -> Callable:
    """
    Return the function for copying the test case log files.

    Parameters
    ----------
    copy_log_file : function
        Function which copies log files
    get_test_data_path : Path
        Path to test data
    yield_logs : dict
        Dict containing paths to logs (these will be copied by copy_log_file)

    Returns
    -------
    _copy_test_case_log_file : function
        Function which copy the test case log files
    """

    def _copy_test_case_log_file(test_case: str) -> None:
        """
        Copy the test case log files.

        Parameters
        ----------
        test_case : str
            Description of the test on the form

            >>> ('<log_file_present>_<pid_present_in_log>_'
            ...  '<started_time_present_in_log>_'
            ...  '<ended_time_present_in_log>'
            ...  '_<whether_pid_exists>_<new_status>')
        """
        success_log_name = yield_logs["success_log"].name
        failed_log_name = yield_logs["fail_log"].name
        # This corresponds to the names in the `run` table in `test.db`
        name_where_status_is_running = "testdata_5"
        name_where_status_is_submitted = "testdata_6"
        copy_log_file(
            yield_logs["unfinished_started_log_pid_11"].name,
            name_where_status_is_running,
        )
        if "no_log" in test_case:
            # Copy directory and file, then deleting file in order for
            # the destructor to delete the dir
            copy_log_file(success_log_name, name_where_status_is_submitted)
            get_test_data_path.joinpath(
                name_where_status_is_submitted, success_log_name
            ).unlink()
        else:
            # A log file should be copied
            if "no_pid" in test_case:
                copy_log_file(
                    yield_logs["unfinished_no_pid_log"].name,
                    name_where_status_is_submitted,
                )
            else:
                if "not_started" in test_case:
                    copy_log_file(
                        yield_logs["unfinished_not_started_log"].name,
                        name_where_status_is_submitted,
                    )
                else:
                    if "not_ended" in test_case:
                        copy_log_file(
                            yield_logs["unfinished_started_log"].name,
                            name_where_status_is_submitted,
                        )
                    else:
                        if "error" in test_case:
                            copy_log_file(
                                failed_log_name, name_where_status_is_submitted
                            )
                        else:
                            copy_log_file(
                                success_log_name, name_where_status_is_submitted
                            )

    return _copy_test_case_log_file
