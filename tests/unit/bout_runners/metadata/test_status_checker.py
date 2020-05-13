"""Contains unittests for the StatusChecker."""


from pathlib import Path
import psutil
from bout_runners.log.log_reader import LogReader
from bout_runners.metadata.status_checker import StatusChecker


def test_check_if_running_or_errored(monkeypatch,
                                     make_test_database,
                                     yield_logs):
    success_log = LogReader(yield_logs['success_log'])
    unfinished_log = LogReader(yield_logs['unfinished_log'])
    db_connector = make_test_database('check_if_running_or_errored')

    def pid_exists_mock(pid):
        return True if pid == 1190 else False

    monkeypatch.setattr(psutil, 'pid_exists', pid_exists_mock)

    status_checker = StatusChecker(db_connector, Path())

    assert status_checker.check_if_running_or_errored(success_log) ==\
        'running'

    # Will throw error since no pid is present
    assert \
        status_checker.check_if_running_or_errored(unfinished_log) ==\
        'error'
