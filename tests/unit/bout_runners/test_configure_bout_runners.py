"""Contains unittests for the config module."""


from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from bout_runners.configure_bout_runners import (
    set_bout_directory,
    set_log_file_directory,
    set_log_level,
)
from bout_runners.utils.logs import get_log_config
from bout_runners.utils.paths import (
    get_bout_directory,
    get_bout_runners_configuration,
    get_log_file_directory,
)


def test_set_log_level(get_mock_config_path: Path, monkeypatch: MonkeyPatch) -> None:
    """
    Test that the log level is changeable.

    Parameters
    ----------
    get_mock_config_path : Path
        The mocked config directory
    monkeypatch : MonkeyPatch
        MonkeyPatch from pytest
    """
    # Test with parameter input
    _ = get_mock_config_path
    level = "CRITICAL"
    level_number = 5
    set_log_level(level)
    config = get_log_config()
    assert config["handlers"]["file_handler"]["level"] == level
    assert config["handlers"]["console_handler"]["level"] == level
    assert config["root"]["level"] == level

    # Test with incorrect input
    with pytest.raises(ValueError):
        set_log_level("Not a level")

    # Test with empty input
    monkeypatch.setattr("builtins.input", lambda _: "")
    set_log_level()
    config = get_log_config()
    assert config["handlers"]["file_handler"]["level"] == level
    assert config["handlers"]["console_handler"]["level"] == level
    assert config["root"]["level"] == level

    # Test with non-empty input
    level = "ERROR"
    level_number = 3
    monkeypatch.setattr("builtins.input", lambda _: level_number)
    set_log_level()
    config = get_log_config()
    assert config["handlers"]["file_handler"]["level"] == level
    assert config["handlers"]["console_handler"]["level"] == level
    assert config["root"]["level"] == level


def test_set_log_file_directory(
    get_mock_config_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """
    Test that the log file directory is changeable.

    Parameters
    ----------
    get_mock_config_path : Path
        The mocked config directory
    monkeypatch : MonkeyPatch
        MonkeyPatch from pytest
    """
    config_path = get_mock_config_path
    original_dir = get_log_file_directory()

    # Test with empty input
    monkeypatch.setattr("builtins.input", lambda _: "")
    set_log_file_directory()
    config = get_bout_runners_configuration()
    assert config["log"]["directory"] == str(original_dir)

    # Test with parameter input
    log_dir = config_path.joinpath("test_with_parameter")
    set_log_file_directory(log_dir)
    config = get_bout_runners_configuration()
    assert config["log"]["directory"] == str(log_dir)

    # Test with non-empty input
    log_dir = config_path.joinpath("test_with_parameter_2")
    monkeypatch.setattr("builtins.input", lambda _: str(log_dir))
    set_log_file_directory()
    config = get_bout_runners_configuration()
    assert config["log"]["directory"] == str(log_dir)


def test_set_bout_directory(
    get_mock_config_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """
    Test that the BOUT++ directory is changeable.

    Parameters
    ----------
    get_mock_config_path : Path
        The mocked config directory
    monkeypatch : MonkeyPatch
        MonkeyPatch from pytest
    """
    _ = get_mock_config_path
    original_dir = get_bout_directory()

    # Test with empty input
    monkeypatch.setattr("builtins.input", lambda _: "")
    set_bout_directory()
    config = get_bout_runners_configuration()
    assert config["bout++"]["directory"] == str(original_dir)

    # Test with incorrect input
    with pytest.raises(ValueError):
        set_bout_directory(original_dir.joinpath("not", "a", "dir"))

    # Test with parameter input
    bout_dir = original_dir.parent
    set_bout_directory(bout_dir)
    config = get_bout_runners_configuration()
    assert config["bout++"]["directory"] == str(bout_dir)

    # Test with non-empty input
    monkeypatch.setattr("builtins.input", lambda _: str(original_dir))
    set_bout_directory()
    config = get_bout_runners_configuration()
    assert config["bout++"]["directory"] == str(original_dir)
