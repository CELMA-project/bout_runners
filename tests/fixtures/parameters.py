"""Contains fixtures for the parameters class."""


from pathlib import Path

import pytest

from bout_runners.parameters.default_parameters import DefaultParameters


@pytest.fixture(scope="session", name="get_default_parameters")
def fixture_get_default_parameters(get_test_data_path: Path) -> DefaultParameters:
    """
    Return the default parameters object.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data

    Returns
    -------
    default_parameters : DefaultParameters
        The DefaultParameters object
    """
    settings_path = get_test_data_path.joinpath("BOUT.settings")
    default_parameters = DefaultParameters(settings_path=settings_path)
    return default_parameters
