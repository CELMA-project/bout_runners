"""Contains unittests for the StatusChecker."""

import signal
import functools
from pathlib import Path
from datetime import datetime
import pytest
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.metadata.status_checker import StatusChecker


def test_status_checker_run_time_error(make_test_database):
    """
    Test that the status checker raises RuntimeError without tables.

    Parameters
    ----------
    make_test_database : DatabaseConnector
        Connection to the test database
    """
    db_connector = make_test_database('status_checker_no_table')
    status_checker = StatusChecker(db_connector, Path())

    with pytest.raises(RuntimeError):
        status_checker.check_and_update_status()


@pytest.mark.parametrize(
    'test_case',
    ('no_log_file_no_pid_not_started_not_ended_no_mock_pid_submitted',
     'log_file_no_pid_not_started_not_ended_no_mock_pid_created',
     'log_file_pid_not_started_not_ended_no_mock_pid_error',
     'log_file_pid_not_started_not_ended_mock_pid_running',
     'log_file_pid_started_not_ended_no_mock_pid_error',
     'log_file_pid_started_not_ended_mock_pid_running',
     'log_file_pid_started_ended_no_mock_pid_error',
     'log_file_pid_started_ended_no_mock_pid_complete'))
def test_status_checker(test_case,
                        get_test_data_path,
                        get_test_db_copy,
                        mock_pid_exists,
                        copy_test_case_log_file):
    """
    Test the StatusChecker exhaustively (excluding raises and loop).

    Parameters
    ----------
    test_case : str
        Description of the test on the form
        >>> ('<log_file_present>_<pid_present_in_log>_'
        ...  '<started_time_present_in_log>_<ended_time_present_in_log>'
        ...  '_<whether_pid_exists>_<new_status>')
    get_test_data_path : Path
        Path to test data
    get_test_db_copy : function
        Function which returns a a database connector to the copy of the
        test database
    mock_pid_exists : function
        Function which sets up a monkeypatch for psutil.pid_exist
    copy_test_case_log_file : function
        Function which copies log files according to the test_case
    """
    project_path = get_test_data_path
    db_connector = get_test_db_copy(test_case)
    mock_pid_exists(test_case)
    copy_test_case_log_file(test_case)

    db_reader = DatabaseReader(db_connector)

    status_checker = StatusChecker(db_connector, project_path)
    status_checker.check_and_update_status()

    # Check that the correct status has been assigned to "submitted"
    expected = test_case.split('_')[-1]
    # pylint: disable=no-member
    result = db_reader.query(
        "SELECT latest_status FROM run WHERE name = "
        "'testdata_6'").loc[0, 'latest_status']
    assert result == expected

    # Check that the correct status has been assigned to "running"
    # pylint: disable=no-member
    result = db_reader.query(
        "SELECT latest_status FROM run WHERE name = "
        "'testdata_5'").loc[0, 'latest_status']
    assert result == 'running'

    # Check that correct start_time has been set
    if 'not_started' not in test_case:
        expected = datetime(2020, 5, 1, 17, 7, 10)
        # pylint: disable=no-member
        result = db_reader.query(
            "SELECT start_time FROM run WHERE name = "
            "'testdata_6'"
        ).loc[0, 'start_time']
        assert str(expected) == result

    # Check that correct end_time has been set
    if 'not_ended' not in test_case and 'complete' in test_case:
        expected = datetime(2020, 5, 1, 17, 7, 14)
        # pylint: disable=no-member
        result = db_reader.query(
            "SELECT stop_time FROM run WHERE name = "
            "'testdata_6'"
        ).loc[0, 'stop_time']
        assert str(expected) == result


# https://www.saltycrane.com/blog/2010/04/using-python-timeout-decorator-uploading-s3/
def timeout(seconds_before_timeout):
    def decorate(f):
        def handler(_, __):
            raise TimeoutError()

        @functools.wraps(f)
        def new_f(*args, **kwargs):
            old = signal.signal(signal.SIGALRM, handler)
            signal.alarm(seconds_before_timeout)
            try:
                result = f(*args, **kwargs)
            finally:
                # reinstall the old signal handler
                signal.signal(signal.SIGALRM, old)
                # cancel the alarm
                signal.alarm(0)
            return result
        new_f.__name__ = f.__name__
        return new_f
    return decorate


@timeout(1)
def test_status_checker_until_complete(get_test_data_path,
                                       get_test_db_copy,
                                       mock_pid_exists,
                                       copy_test_case_log_file):
    """
    Test the StatusChecker loop.

    Parameters
    ----------
    get_test_data_path
    get_test_db_copy
    mock_pid_exists
    copy_test_case_log_file
    """
    test_case = \
        'log_file_pid_started_ended_no_mock_pid_complete'

    project_path = get_test_data_path
    db_connector = get_test_db_copy(test_case)
    mock_pid_exists(test_case)
    copy_test_case_log_file(test_case)

    db_reader = DatabaseReader(db_connector)

    status_checker = StatusChecker(db_connector, project_path)
    status_checker.check_and_update_until_complete()

    query = ("SELECT name, id AS run_id FROM run WHERE\n"
             "latest_status = 'submitted' OR\n"
             "latest_status = 'created' OR\n"
             "latest_status = 'running'")
    assert len(db_reader.query(query).index) == 0
