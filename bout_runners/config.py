"""Module for configuring bout_runners."""


import yaml
import logging
from bout_runners.utils.paths import get_log_file_path
from bout_runners.utils.logs import get_log_config
from bout_runners.utils.logs import set_up_logger


def set_log_level(level=None):
    """
    Set the log level.

    Parameters
    ----------
    level : None or str
        The logging level to use
        If None the caller will be prompted
    """
    config = get_log_config()
    possibilities = ('DEBUG',
                     'INFO',
                     'WARNING',
                     'ERROR',
                     'CRITICAL')

    if level is None:
        possibilities_map = \
            {nr: option for nr, option in enumerate(possibilities)}
        question = 'Please set the log level by entering a number:\n'
        for nr, option in possibilities_map.items():
            question += f'{" "*3}({nr}) - {option}\n'
        # Set an answer to start the wile loop
        answer = -1
        possibilities_keys = possibilities_map.keys()
        while answer not in possibilities_keys:
            answer = int(input(question))
    else:
        if level not in possibilities:
            msg = (f'`level` in `set_log_level` must be one of '
                   f'{possibilities}')
            raise ValueError(msg)

    config['handlers']['file_handler']['level'] = level
    config['handlers']['console_handler']['level'] = level

    with get_log_file_path(name='bout_runners.log').open() as log_file:
        log_file.write(yaml.dump(config))

    set_up_logger(config)

    logging.info('Logging level set to %s', level)

# FIXME: If this is to be implemented, the log path needs also to be
#  set in bout++.ini
def set_log_path(log_path):
    """
    Set the directory of the log files.
    """
    pass


def set_bout_path(bout_path=None):
    """
    Set the path to the BOUT++ directory.

    Parameters
    ----------
    bout_path : None or Path
        The path to the BOUT++ directory
        If None, the caller will be prompted
    """
    pass


if __name__ == '__main__':
    pass