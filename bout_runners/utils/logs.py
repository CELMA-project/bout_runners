"""Contains methods which deals with logging."""


import logging
import logging.config
from typing import Any, Dict, Optional

import yaml
from bout_runners.utils.paths import get_log_file_path, get_logger_config_path


# NOTE: Looks like mypy has trouble with recursive objects, thus this using Any looks
#       like a good solution for now
#       See also
#       https://github.com/python/typing/issues/182
def get_log_config() -> Dict[str, Any]:
    """
    Get the logging configuration.

    Returns
    -------
    config : dict
        A dictionary containing the logging configuration
    """
    log_config_path = get_logger_config_path()

    with log_config_path.open("r") as config_file:
        config: Dict[str, Any] = yaml.safe_load(config_file.read())
    return config


def set_up_logger(config: Optional[Dict[str, Any]] = None) -> None:
    """
    Set up the logger.

    Parameters
    ----------
    config : None or dict
        A dictionary containing the logging configuration
    """
    if config is None:
        config = get_log_config()
    config["handlers"]["file_handler"]["filename"] = str(
        get_log_file_path(name="bout_runners.log")
    )
    logging.config.dictConfig(config)
