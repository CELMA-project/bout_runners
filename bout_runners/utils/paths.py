"""Contains methods which return common paths."""
import os
import time
from pathlib import Path

from dotenv import load_dotenv


def get_root_path():
    """
    Return the absolute path to the root of this repository.

    Returns
    -------
    Path
        The path to the root directory
    """
    return Path(__file__).absolute().parents[2]


def get_bout_runners_path():
    """
    Return the absolute path to the bout_runners package.

    Returns
    -------
    Path
        The path to the reports bout_runners
    """
    return get_root_path().joinpath('bout_runners')


def get_logger_path():
    """
    Return the absolute path to the logger configuration.

    Returns
    -------
    Path
        The path to the logger configuration file
    """
    return get_bout_runners_path().joinpath('logging_config.yaml')


def get_log_file_path(name=time.strftime('%Y%m%d.log')):
    """
    Return the absolute path to the log file path.

    Parameters
    ----------
    name : str
        Name of the log file

    Returns
    -------
    log_file_path : Path
        The path to the log file
    """
    logfile_dir = get_root_path().joinpath('logs')
    logfile_dir.mkdir(exist_ok=True, parents=True)

    log_file_path = logfile_dir.joinpath(name)

    return log_file_path


def get_bout_path():
    """
    Load the dot-env file and yield the bout_path.

    Returns
    -------
    bout_path : Path
        Path to the BOUT++ repository
    """
    # Setup
    load_dotenv()
    bout_path = Path(os.getenv('BOUT_PATH')).absolute()
    return bout_path