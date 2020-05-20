"""Contains unittests for the config module."""


import pytest
from bout_runners.config import set_log_level
from bout_runners.utils.logs import get_log_config


def test_set_log_level(protect_config):
    """
    Test that the log level is changeable.

    Parameters
    ----------
    protect_config : tuple
        Tuple containing the config_path and copied_path
    """
    _ = protect_config
    level = 'CRITICAL'
    set_log_level(level)
    config = get_log_config()
    assert config['handlers']['file_handler']['level'] == level
    assert config['handlers']['console_handler']['level'] == level

    with pytest.raises(ValueError):
        set_log_level('Not a level')
