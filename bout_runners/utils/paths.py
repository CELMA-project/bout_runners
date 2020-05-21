"""Contains methods which return common paths."""


import time
import configparser
from pathlib import PosixPath, Path
from configparser import ConfigParser
from typing import Optional


def get_root_path() -> PosixPath:
    """
    Return the absolute path to the root of this repository.

    Returns
    -------
    Path
        The path to the root directory
    """
    return Path(__file__).absolute().parents[2]


def get_config_path() -> PosixPath:
    """
    Return the absolute path to the configurations.

    Returns
    -------
    Path
        The path to the configuration directory
    """
    return get_root_path().joinpath("config")


def get_logger_config_path() -> PosixPath:
    """
    Return the absolute path to the logger configuration.

    Returns
    -------
    Path
        The path to the logger configuration file
    """
    return get_config_path().joinpath("logging_config.yaml")


def get_bout_runners_config_path() -> PosixPath:
    """
    Return the absolute path to the bout_runners configuration.

    Returns
    -------
    Path
        The path to the bout_runners configuration file
    """
    return get_config_path().joinpath("bout_runners.ini")


def get_bout_log_config_path() -> PosixPath:
    """
    Return the absolute path to the log configuration.

    Returns
    -------
    Path
        The path to the bout_runners configuration file
    """
    return get_config_path().joinpath("logging_config.yaml")


def get_bout_runners_configuration() -> ConfigParser:
    """
    Return the bout_runners configuration.

    Returns
    -------
    config : FIXME
        The configuration of bout_runners
    """
    config = configparser.ConfigParser()
    config.read(get_bout_runners_config_path())
    return config


def get_log_file_directory() -> PosixPath:
    """
    Return the log_file directory.

    Returns
    -------
    log_file_directory : Path
        Path to the log_file directory
    """
    config = get_bout_runners_configuration()
    path_str = config["log"]["directory"]
    if path_str == "None":
        log_file_dir = get_root_path().joinpath("logs")
    else:
        log_file_dir = Path(path_str)

    log_file_dir.mkdir(exist_ok=True, parents=True)
    return log_file_dir


def get_log_file_path(
    log_file_dir: None = None, name: Optional[str] = None
) -> PosixPath:
    """
    Return the absolute path to the log file path.

    Parameters
    ----------
    log_file_dir : Path or None
        Path to the log file directory
        If None, default log file directory will be used
    name : str or None
        Name of the log file
        If None, current date will be used

    Returns
    -------
    log_file_path : Path
        The path to the log file
    """
    log_file_dir = (
        log_file_dir if log_file_dir is not None else get_log_file_directory()
    )
    name = name if name is not None else time.strftime("%Y%m%d.log")
    log_file_path = log_file_dir.joinpath(name)

    return log_file_path


def get_bout_directory() -> PosixPath:
    """
    Load the BOUT++ directory from the configuration file.

    Returns
    -------
    bout_path : Path
        Path to the BOUT++ repository
    """
    config = get_bout_runners_configuration()
    path_str = config["bout++"]["directory"]
    if "$HOME/" or "${HOME}/" in path_str.lower():
        path_str = "/".join(path_str.split("/")[1:])
        path_str = f"{Path.home()}/{path_str}"
    bout_path = Path(path_str).absolute()
    return bout_path
