"""Contains methods which return common paths."""


import time
import configparser
from pathlib import Path


def get_root_path():
    """
    Return the absolute path to the root of this repository.

    Returns
    -------
    Path
        The path to the root directory
    """
    return Path(__file__).absolute().parents[2]


def get_config_path():
    """
    Return the absolute path to the configurations.

    Returns
    -------
    Path
        The path to the configuration directory
    """
    return get_root_path().joinpath('config')


def get_logger_config_path():
    """
    Return the absolute path to the logger configuration.

    Returns
    -------
    Path
        The path to the logger configuration file
    """
    return get_config_path().joinpath('logging_config.yaml')


def get_bout_runners_config_path():
    """
    Return the absolute path to the bout_runners configuration.

    Returns
    -------
    Path
        The path to the bout_runners configuration file
    """
    return get_config_path().joinpath('bout_runners.ini')


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
    config = configparser.ConfigParser()
    config.read(get_bout_runners_config_path())
    path_str = config['bout++']['path']
    if '$HOME/' or '${HOME}/' in path_str.lower():
        path_str = '/'.join(path_str.split('/')[1:])
        path_str = f'{Path.home()}/{path_str}'
    bout_path = Path(path_str).absolute()
    return bout_path
