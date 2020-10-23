"""Contains methods which return common paths."""


import configparser
import time
from pathlib import Path
from typing import Optional


def get_bout_runners_package_path() -> Path:
    """
    Return the absolute path to the bout_runners package.

    Returns
    -------
    Path
        The path to the root directory
    """
    return Path(__file__).absolute().parents[1]


def get_config_path() -> Path:
    """
    Return the absolute path to the configurations.

    Returns
    -------
    Path
        The path to the configuration directory
    """
    return get_bout_runners_package_path().joinpath("config")


def get_logger_config_path() -> Path:
    """
    Return the absolute path to the logger configuration.

    Returns
    -------
    Path
        The path to the logger configuration file
    """
    return get_config_path().joinpath("logging_config.yaml")


def get_bout_runners_config_path() -> Path:
    """
    Return the absolute path to the bout_runners configuration.

    Returns
    -------
    Path
        The path to the bout_runners configuration file
    """
    return get_config_path().joinpath("bout_runners.ini")


def get_default_submitters_config_path() -> Path:
    """
    Return the absolute path to the default submitters configuration.

    Returns
    -------
    Path
        The default path to the submitters configuration file
    """
    return get_config_path().joinpath("submitters.ini")


def get_submitters_config_path() -> Path:
    """
    Return the path to the submitter configuration.

    Returns
    -------
    submitters_config_path : Path
        Path to the submitters configuration
    """
    config = get_bout_runners_configuration()
    path_str = config["submitter_config"]["directory"]
    if path_str.lower() == "none":
        submitters_config_path = get_default_submitters_config_path()
    else:
        submitters_config_path = Path(path_str)

    return submitters_config_path


def get_submitters_configuration() -> configparser.ConfigParser:
    """
    Return the submitters configuration.

    Returns
    -------
    config : configparser.ConfigParser
        The submitter configuration
    """
    config = configparser.ConfigParser()
    config.read(get_submitters_config_path())
    return config


def get_bout_runners_configuration() -> configparser.ConfigParser:
    """
    Return the bout_runners configuration.

    Returns
    -------
    config : configparser.ConfigParser
        The configuration of bout_runners
    """
    config = configparser.ConfigParser()
    config.read(get_bout_runners_config_path())
    return config


def get_log_file_directory() -> Path:
    """
    Return the log_file directory.

    Returns
    -------
    log_file_directory : Path
        Path to the log_file directory
    """
    config = get_bout_runners_configuration()
    path_str = config["log"]["directory"]
    if path_str.lower() == "none":
        log_file_dir = get_bout_runners_package_path().joinpath("logs")
    else:
        log_file_dir = Path(path_str)

    log_file_dir.mkdir(exist_ok=True, parents=True)
    return log_file_dir


def get_log_file_path(
    log_file_dir: Optional[Path] = None, name: Optional[str] = None
) -> Path:
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
    if log_file_dir is None:
        log_file_dir = get_log_file_directory()
    if name is None:
        name = time.strftime("%Y%m%d.log")
    log_file_path = log_file_dir.joinpath(name)

    return log_file_path


def get_bout_directory() -> Path:
    """
    Load the BOUT++ directory from the configuration file.

    Returns
    -------
    bout_path : Path
        Path to the BOUT++ repository
    """
    config = get_bout_runners_configuration()
    path_str = config["bout++"]["directory"]
    if (
        "$home/" in path_str.lower()
        or "${home}/" in path_str.lower()
        or "$(home)/" in path_str.lower()
    ):
        path_str = "/".join(path_str.split("/")[1:])
        path_str = f"{Path.home()}/{path_str}"
    bout_path = Path(path_str).absolute()
    return bout_path
