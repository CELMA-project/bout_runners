"""Contains methods which deals with logging."""


import yaml
import logging
import logging.config
from bout_runners.utils.paths import get_logger_path
from bout_runners.utils.paths import get_log_file_path


def set_up_logger():
    """Set up the logger."""
    log_config_path = get_logger_path()

    with log_config_path.open('r') as f:
        config = yaml.safe_load(f.read())
    config['handlers']['file_handler']['filename'] = \
        str(get_log_file_path(name='bout_runners.log'))
    logging.config.dictConfig(config)
