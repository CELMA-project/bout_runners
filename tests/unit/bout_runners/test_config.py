"""Contains unittests for the config module."""


import pytest
from bout_runners.config import set_log_level
from bout_runners.config import set_log_file_directory
from bout_runners.utils.logs import get_log_config
from bout_runners.utils.paths import get_bout_runners_configuration


def test_set_log_level(protect_config, monkeypatch):
    """
    Test that the log level is changeable.

    Parameters
    ----------
    protect_config : tuple
        Tuple containing the config_path and copied_path
    monkeypatch : MonkeyPatch
        MonkeyPatch from pytest
    """
    # Test with parameter input
    _ = protect_config
    level = 'CRITICAL'
    level_number = 5
    set_log_level(level)
    config = get_log_config()
    assert config['handlers']['file_handler']['level'] == level
    assert config['handlers']['console_handler']['level'] == level
    assert config['root']['level'] == level

    # Test with incorrect input
    with pytest.raises(ValueError):
        set_log_level('Not a level')

    # Test with empty input
    monkeypatch.setattr('builtins.input', lambda _: None)
    set_log_level()
    config = get_log_config()
    assert config['handlers']['file_handler']['level'] == level
    assert config['handlers']['console_handler']['level'] == level
    assert config['root']['level'] == level

    # Test with non-empty input
    level = 'ERROR'
    level_number = 3
    monkeypatch.setattr('builtins.input', lambda _: level_number)
    set_log_level()
    config = get_log_config()
    assert config['handlers']['file_handler']['level'] == level
    assert config['handlers']['console_handler']['level'] == level
    assert config['root']['level'] == level


def test_set_log_file_directory(protect_config, monkeypatch):
    """
    Test that the log file directory is changeable.

    Parameters
    ----------
    protect_config : tuple
        Tuple containing the config_path and copied_path
    monkeypatch : MonkeyPatch
        MonkeyPatch from pytest
    """
    config_path, _ = protect_config
    log_dir = config_path.joinpath('test_with_parameter')

    # Test with parameter input
    set_log_file_directory(log_dir)
    config = get_bout_runners_configuration()
    assert config['bout++']['directory'] == str(log_dir)

    # Test with empty input
    monkeypatch.setattr('builtins.input', lambda _: None)
    set_log_file_directory()
    config = get_log_config()
    assert config['bout++']['directory'] == str(log_dir)

    # Test with non-empty input
    monkeypatch.setattr('builtins.input', lambda _: str(log_dir))
    set_log_file_directory()
    config = get_log_config()
    assert config['bout++']['directory'] == str(log_dir)
