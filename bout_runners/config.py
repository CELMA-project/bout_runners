"""Module for configuring bout_runners."""


import logging
from pathlib import Path
import yaml
from bout_runners.utils.paths import get_bout_runners_configuration
from bout_runners.utils.paths import get_bout_runners_config_path
from bout_runners.utils.paths import get_bout_directory
from bout_runners.utils.paths import get_log_file_directory
from bout_runners.utils.paths import get_bout_log_config_path
from bout_runners.utils.logs import get_log_config
from bout_runners.utils.logs import set_up_logger
from typing import Optional


def set_log_level(level: Optional[str] = None) -> None:
    """
    Set the log level.

    Parameters
    ----------
    level : None or str
        The logging level to use
        If None the caller will be prompted
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
            if answer_input is not None:
                answer_int = int(answer_input)
            if answer_input is None:
                # Reverse the dict
                answer_int = list(possibilities_map.keys())[
                    list(possibilities_map.values()).index(current_level)
                ]
                break
        level = possibilities_map[answer_int]

    if level not in possibilities:
        msg = f"`level` in `set_log_level` must be one of " f"{possibilities}"
        raise ValueError(msg)

    config["handlers"]["file_handler"]["level"] = level
    config["handlers"]["console_handler"]["level"] = level
    config["root"]["level"] = level

    with get_bout_log_config_path().open("w") as log_file:
        log_file.write(yaml.dump(config))

    set_up_logger(config)
    logging.info("Logging level set to %s", level)


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
            f"Please entering the directory for log files:\n"
            f"Empty input will reuse the current directory "
            f"{current_dir}\n"
        )
        answer = input(question)
        if answer is None:
            config["log"]["directory"] = str(current_dir)
        else:
            config["log"]["directory"] = answer
    else:
        config["log"]["directory"] = str(log_dir)

    with get_bout_runners_config_path().open("w") as configfile:
        config.write(configfile)

    set_up_logger()
    logging.info("Logging directory set to %s", config["log"]["directory"])


def set_bout_directory(bout_dir: Optional[Path] = None) -> None:
    """
    Set the path to the BOUT++ directory.

    Parameters
    ----------
    bout_dir : None or Path
        The path to the BOUT++ directory
        If None, the caller will be prompted
    """
    config = get_bout_runners_configuration()
    if bout_dir is None:
        current_dir = get_bout_directory()
        question = (
            f"Please entering the directory for the root of BOUT++:\n"
            f"Empty input will reuse the current directory "
            f"{current_dir}\n"
        )
        answer = input(question)
        if answer is None:
            config["bout++"]["directory"] = str(current_dir)
        else:
            config["bout++"]["directory"] = answer
    else:
        config["bout++"]["directory"] = str(bout_dir)

    if not Path(config["bout++"]["directory"]).is_dir():
        msg = f'BOUT++ not found in {config["bout++"]["directory"]}'
        raise ValueError(msg)

    with get_bout_runners_config_path().open("w") as configfile:
        config.write(configfile)

    set_up_logger()
    logging.info("BOUT++ directory set to %s", config["log"]["directory"])


if __name__ == "__main__":
    set_log_level()
    set_log_file_directory()
    set_bout_directory()
