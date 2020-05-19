"""Contains unittests for the log_reader."""


from datetime import datetime
from bout_runners.log.log_reader import LogReader


def test_started(yield_logs):
    """
    Test self.started.

    Parameters
    ----------
    yield_logs : dict of Path
        A dictionary containing the log paths used for testing
    """
    log_paths = yield_logs
    success_log_reader = LogReader(log_paths['success_log'])
    assert success_log_reader.started()

    failed_log_reader = LogReader(log_paths['fail_log'])
    assert failed_log_reader.started()

    unfinished_no_pid_log_reader = \
        LogReader(log_paths['unfinished_no_pid_log'])
    assert unfinished_no_pid_log_reader.started() is False


def test_ended(yield_logs):
    """
    Test self.ended.

    Parameters
    ----------
    yield_logs : dict of Path
        A dictionary containing the log paths used for testing
    """
    log_paths = yield_logs
    success_log_reader = LogReader(log_paths['success_log'])
    assert success_log_reader.ended()

    failed_log_reader = LogReader(log_paths['fail_log'])
    assert failed_log_reader.ended() is False

    unfinished_no_pid_log_reader = \
        LogReader(log_paths['unfinished_no_pid_log'])
    assert unfinished_no_pid_log_reader.ended() is False


def test_pid_exist(yield_logs):
    """
    Test self.pid_exist.

    Parameters
    ----------
    yield_logs : dict of Path
        A dictionary containing the log paths used for testing
    """
    log_paths = yield_logs
    success_log_reader = LogReader(log_paths['success_log'])
    assert success_log_reader.pid_exist()

    failed_log_reader = LogReader(log_paths['fail_log'])
    assert failed_log_reader.pid_exist()

    unfinished_no_pid_log_reader = \
        LogReader(log_paths['unfinished_no_pid_log'])
    assert unfinished_no_pid_log_reader.ended() is False


def test_start_time(yield_logs):
    """
    Test self.start_time.

    Parameters
    ----------
    yield_logs : dict of Path
        A dictionary containing the log paths used for testing
    """
    log_paths = yield_logs
    start_time = datetime(2020, 5, 1, 17, 7, 10)
    success_log_reader = LogReader(log_paths['success_log'])
    assert success_log_reader.start_time == start_time

    failed_log_reader = LogReader(log_paths['fail_log'])
    assert failed_log_reader.start_time == start_time

    unfinished_no_pid_log_reader = \
        LogReader(log_paths['unfinished_no_pid_log'])
    assert unfinished_no_pid_log_reader.start_time is None


def test_end_time(yield_logs):
    """
    Test self.end_time.

    Parameters
    ----------
    yield_logs : dict of Path
        A dictionary containing the log paths used for testing
    """
    log_paths = yield_logs
    end_time = datetime(2020, 5, 1, 17, 7, 14)
    success_log_reader = LogReader(log_paths['success_log'])
    assert success_log_reader.end_time == end_time

    failed_log_reader = LogReader(log_paths['fail_log'])
    assert failed_log_reader.end_time is None

    unfinished_no_pid_log_reader = \
        LogReader(log_paths['unfinished_no_pid_log'])
    assert unfinished_no_pid_log_reader.end_time is None


def test_pid(yield_logs):
    """
    Test self.pid.

    Parameters
    ----------
    yield_logs : dict of Path
        A dictionary containing the log paths used for testing
    """
    log_paths = yield_logs
    success_log_reader = LogReader(log_paths['success_log'])
    assert success_log_reader.pid == 1191

    failed_log_reader = LogReader(log_paths['fail_log'])
    assert failed_log_reader.pid == 1190

    unfinished_no_pid_log_reader = \
        LogReader(log_paths['unfinished_no_pid_log'])
    assert unfinished_no_pid_log_reader.pid is None
