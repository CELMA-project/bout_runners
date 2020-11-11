"""Contains unittests for the config module."""


import configparser
import shutil
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from bout_runners.configure_bout_runners import (
    set_bout_directory,
    set_log_file_directory,
    set_log_level,
    set_submitter_config_path,
)
from bout_runners.utils.logs import get_log_config
from bout_runners.utils.paths import (
    get_bout_directory,
    get_bout_runners_configuration,
    get_default_submitters_config_path,
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
    log_dir = config_path.joinpath("test_set_log_dir_with_parameter")
    set_log_file_directory(log_dir)
    config = get_bout_runners_configuration()
    assert config["log"]["directory"] == str(log_dir)

    # Test with non-empty input
    log_dir = config_path.joinpath("test_set_log_dir_parameter_2")
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


def test_set_submitter_config_path(
    get_mock_config_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """
    Test that the submitter configuration path is changeable.

    Parameters
    ----------
    get_mock_config_path : Path
        The mocked config directory
    monkeypatch : MonkeyPatch
        MonkeyPatch from pytest
    """
    config_path = get_mock_config_path
    original_path = get_default_submitters_config_path()

    # Test with empty input
    monkeypatch.setattr("builtins.input", lambda _: "")
    set_submitter_config_path()
    config = get_bout_runners_configuration()
    assert config["submitter_config"]["path"] == str(original_path)

    # Test with parameter input
    submitter_path = config_path.joinpath(
        "test_set_submitter_with_parameter", "settings.ini"
    )
    set_submitter_config_path(submitter_path)
    config = get_bout_runners_configuration()
    assert config["submitter_config"]["path"] == str(submitter_path)

    # Test with non-empty input
    submitter_path = config_path.joinpath(
        "test_set_submitter_with_parameter", "settings_2.ini"
    )
    monkeypatch.setattr("builtins.input", lambda _: str(submitter_path))
    set_submitter_config_path()
    config = get_bout_runners_configuration()
    assert config["submitter_config"]["path"] == str(submitter_path)

    # Test raises
    new_path = config_path.joinpath("test_set_submitter_raises", "settings.ini")
    new_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(original_path, new_path)
    new_config = configparser.ConfigParser()
    new_config.read(new_path)
    new_config.remove_option("local", "number_of_processors")
    with new_path.open("w") as configfile:
        new_config.write(configfile)

    with pytest.raises(ValueError):
        set_submitter_config_path(new_path)

    new_config.remove_section("local")
    with new_path.open("w") as configfile:
        new_config.write(configfile)

    with pytest.raises(ValueError):
        set_submitter_config_path(new_path)
