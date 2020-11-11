"""Module for configuring bout_runners."""


import configparser
import logging
import shutil
from pathlib import Path
from typing import Optional

import yaml

from bout_runners.utils.logs import get_log_config, set_up_logger
from bout_runners.utils.paths import (
    get_bout_directory,
    get_bout_runners_config_path,
    get_bout_runners_configuration,
    get_default_submitters_config_path,
    get_log_file_directory,
    get_logger_config_path,
)


def set_log_level(level: Optional[str] = None) -> None:
    """
    Set the log level.

    Parameters
    ----------
    level : None or str
        The logging level to use
        If None the caller will be prompted

    Raises
    ------
    ValueError
        If the level is not one of the possibilities
    """
    config = get_log_config()

    possibilities = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

    if level is None:
        current_level = config["root"]["level"]

        possibilities_map = dict(enumerate(possibilities))
        question = (
            f"Please set the log level by entering a number:\n"
            f"   (empty input will reuse the current level "
            f"[{current_level}])\n"
        )
        for key, val in possibilities_map.items():
            question += f'{" "*3}({key}) - {val}\n'
        # Set an answer to start the wile loop
        answer_int = -1
        possibilities_keys = possibilities_map.keys()
        while answer_int not in possibilities_keys:
            answer_input = input(question)
            print(f"Your answered: '{answer_input}'")
            if answer_input != "":
                answer_int = int(answer_input)
            if answer_input == "":
                # Reverse the dict
                answer_int = list(possibilities_map.keys())[
                    list(possibilities_map.values()).index(current_level)
                ]
                break
        level = possibilities_map[answer_int]

    if level not in possibilities:
        msg = f"`level` in `set_log_level` must be one of " f"{possibilities}"
        raise ValueError(msg)

    print(f"Setting logging level to {level}")

    config["handlers"]["file_handler"]["level"] = level
    config["handlers"]["console_handler"]["level"] = level
    config["root"]["level"] = level

    with get_logger_config_path().open("w") as log_file:
        log_file.write(yaml.dump(config))

    set_up_logger(config)
    logging.debug("Logging level set to %s", level)
    print("")


def set_log_file_directory(log_dir: Optional[Path] = None) -> None:
    """
    Set the directory of the log files.

    Parameters
    ----------
    log_dir : None or Path
        The directory to keep the log files
        If None the caller will be prompted
    """
    config = get_bout_runners_configuration()
    if log_dir is None:
        current_dir = get_log_file_directory()
        question = (
            f"Please enter the directory for log files:\n"
            f"Empty input will reuse the current directory "
            f"{current_dir}\n"
        )
        answer = input(question)
        print(f"Your answered: '{answer}'")
        if answer == "":
            config["log"]["directory"] = str(current_dir)
        else:
            config["log"]["directory"] = answer
    else:
        config["log"]["directory"] = str(log_dir)

    with get_bout_runners_config_path().open("w") as configfile:
        config.write(configfile)

    print(f"Setting logging dir to {config['log']['directory']}")

    set_up_logger()
    logging.debug("Logging directory set to %s", config["log"]["directory"])
    print("")


def set_bout_directory(bout_dir: Optional[Path] = None) -> None:
    """
    Set the path to the BOUT++ directory.

    Parameters
    ----------
    bout_dir : None or Path
        The path to the BOUT++ directory
        If None, the caller will be prompted

    Raises
    ------
    ValueError
        If BOUT++ not found in the directory
    """
    config = get_bout_runners_configuration()
    if bout_dir is None:
        suggested_dir = get_bout_directory()
        question = (
            f"Please enter the directory for the root of BOUT++:\n"
            f"Empty input use the directory "
            f"{suggested_dir}\n"
        )
        answer = input(question)
        print(f"Your answered: '{answer}'")
        if answer != "":
            config["bout++"]["directory"] = answer
        else:
            config["bout++"]["directory"] = str(suggested_dir)
    else:
        config["bout++"]["directory"] = str(bout_dir)

    if not Path(config["bout++"]["directory"]).is_dir():
        msg = f'BOUT++ not found in {config["bout++"]["directory"]}'
        raise ValueError(msg)

    with get_bout_runners_config_path().open("w") as configfile:
        config.write(configfile)

    print(f"Setting BOUT++ directory to {config['bout++']['directory']}")

    set_up_logger()
    logging.debug("BOUT++ directory set to %s", config["bout++"]["directory"])
    print("")


def set_submitter_config_path(submitter_config_path: Optional[Path] = None) -> None:
    """
    Set the path to the submitter configuration.

    Parameters
    ----------
    submitter_config_path : None or Path
        The path to the submitter config file
        If None, the caller will be prompted
    """
    config = get_bout_runners_configuration()
    default_path = get_default_submitters_config_path()
    if submitter_config_path is None:
        question = (
            f"Please enter the path to the submitter configuration file:\n"
            f"Empty input use the path "
            f"{default_path}\n"
        )
        answer = input(question)
        print(f"Your answered: '{answer}'")
        if answer != "":
            config["submitter_config"]["path"] = answer
        else:
            config["submitter_config"]["path"] = str(default_path)
    else:
        config["submitter_config"]["path"] = str(submitter_config_path)

    set_up_logger()

    submitter_config_path = Path(config["submitter_config"]["path"])
    check_submitter_config(submitter_config_path, default_path)

    with get_bout_runners_config_path().open("w") as configfile:
        config.write(configfile)

    print(f"Setting submitter configuration file to {submitter_config_path}")

    logging.debug("submitter configuration file set to %s", submitter_config_path)
    print("")


def check_submitter_config(submitter_config_path: Path, default_path: Path) -> None:
    """
    Check that the submitter configuration file is properly formatted.

    Parameters
    ----------
    submitter_config_path : Path
        Path to the specified submitter configuration path
    default_path : Path
        Path to the default submitter configuration

    Raises
    ------
    ValueError
        If either sections or options are missing from the submitter_config_path
    """
    if not submitter_config_path.is_file():
        logging.warning(
            "Did not find any files in %s, will copy from %s",
            submitter_config_path,
            default_path,
        )
        submitter_config_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(default_path, submitter_config_path)
        return

    default_config = configparser.ConfigParser()
    default_config.read(default_path)

    new_config = configparser.ConfigParser()
    new_config.read(submitter_config_path)

    for section in default_config:
        if section not in new_config:
            raise ValueError(
                f"Could not find section {section} in {submitter_config_path}"
            )
        for option in default_config[section]:
            if option not in new_config[section]:
                raise ValueError(
                    f"Could not find option {option} under section {section} "
                    f"in {submitter_config_path}"
                )


if __name__ == "__main__":
    set_log_level()
    set_log_file_directory()
    set_bout_directory()
    set_submitter_config_path()
