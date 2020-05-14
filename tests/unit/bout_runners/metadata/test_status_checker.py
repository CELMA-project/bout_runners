"""Contains unittests for the StatusChecker."""


from datetime import datetime
from pathlib import Path
import pytest
import psutil
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.log.log_reader import LogReader
from bout_runners.metadata.status_checker import StatusChecker


def test_check_if_running_or_errored(monkeypatch,
                                     make_test_database,
                                     yield_logs):
    success_log = LogReader(yield_logs['success_log'])
    unfinished_no_pid_log = LogReader(yield_logs['unfinished_no_pid_log'])
    db_connector = make_test_database('check_if_running_or_errored')

    def pid_exists_mock(pid):
        return True if pid == 1190 else False

    monkeypatch.setattr(psutil, 'pid_exists', pid_exists_mock)

    status_checker = StatusChecker(db_connector, Path())

    assert status_checker.check_if_running_or_errored(success_log) ==\
        'running'

    # Will return 'error' since no pid is present
    assert \
        status_checker.check_if_running_or_errored(unfinished_no_pid_log) ==\
        'error'


# FIXME: Test for RuntimeError
('no_log_file_no_pid_not_started_not_ended_no_mock_pid_submitted',
 'log_file_no_pid_not_started_not_ended_no_mock_pid_created',
 'log_file_pid_not_started_not_ended_no_mock_pid_error',
 'log_file_pid_not_started_not_ended_mock_pid_running',
 'log_file_pid_started_not_ended_no_mock_pid_error',
 'log_file_pid_started_not_ended_mock_pid_running',
 'log_file_pid_started_ended_no_mock_pid_error',
 'log_file_pid_started_ended_no_mock_pid_complete')
@pytest.mark.parametrize(
    'test_case',
    ('no_log_file_no_pid_not_started_not_ended_no_mock_pid_submitted',
     ))
def test_status_checker(test_case,
                        monkeypatch,
                        get_test_data_path,
                        get_test_db_copy,
                        copy_log_file,
                        yield_logs):
    project_path = get_test_data_path
    db_connector = get_test_db_copy(test_case)
    db_reader = DatabaseReader(db_connector)
    success_log_name = yield_logs['success_log'].name
    failed_log_name = yield_logs['fail_log'].name
    unfinished_no_pid_log_name = \
        yield_logs['unfinished_no_pid_log'].name
    unfinished_not_started_log_name = \
        yield_logs['unfinished_not_started_log'].name
    unfinished_started_log_name = \
        yield_logs['unfinished_started_log'].name

    # This is corresponding to the names in `run` in `test.db`
    running_name = 'testdata_5'
    submitted_name = 'testdata_6'
    copy_log_file(unfinished_started_log_name, running_name)
    # NOTE: We make an exception for the no_log_file case
    if 'no_log' in test_case:
        # Copy directory and file, then deleting file in order for
        # the destructor to delete the dir
        copy_log_file(success_log_name, submitted_name)
        get_test_data_path.joinpath(submitted_name,
                                    success_log_name).unlink()
    else:
        # A log file should be copied
        if 'no_pid' in test_case:
            copy_log_file(unfinished_no_pid_log_name, submitted_name)
        else:
            if 'not_started' in test_case:
                copy_log_file(unfinished_not_started_log_name,
                              submitted_name)
            else:
                if 'not_ended' in test_case:
                    copy_log_file(unfinished_started_log_name,
                                  submitted_name)
                else:
                    if 'error' in test_case:
                        copy_log_file(failed_log_name,
                                      submitted_name)
                    else:
                        copy_log_file(success_log_name,
                                      running_name)

    if 'mock_pid' in test_case:
        def pid_exists_mock(pid):
            return True if pid == 1190 else False
        monkeypatch.setattr(psutil, 'pid_exists', pid_exists_mock)

    status_checker = StatusChecker(db_connector, project_path)
    status_checker.check_and_update_status()

    # Check that the correct status has been assigned to "submitted"
    expected = test_case.split('_')[-1]
    result = db_reader.query(
        f"SELECT latest_status FROM run WHERE name = "
        f"'{submitted_name}'").loc[0, 'latest_status']
    assert result == expected

    # Check that the correct status has been assigned to "running"
    result = db_reader.query(
        f"SELECT latest_status FROM run WHERE name = "
        f"'{running_name}'").loc[0, 'latest_status']
    assert result == 'running'

    # Check that correct start_time has been set
    if 'not_started' not in test_case:
        expected = datetime(2020, 5, 1, 17, 7, 10)
        result = db_reader.query(
            f"SELECT start_time FROM run WHERE name = "
            f"'{submitted_name}'"
        ).loc[0, 'start_time']
        assert expected == result

    # Check that correct end_time has been set
    if 'not_ended' not in test_case and 'complete' in test_case:
        expected = datetime(2020, 5, 1, 17, 7, 14)
        result = db_reader.query(
            'SELECT start_time FROM run WHERE id=7'
        ).loc[:, 'start_time']
        assert expected == result
