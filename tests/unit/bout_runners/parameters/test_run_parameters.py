"""Contains unittests for run parameters."""


import pytest
from bout_runners.parameters.run_parameters import RunParameters


def test_run_parameters():
    """Test that the RunParameters is setting the parameters."""
    run_parameters = RunParameters({'global': {'append': False},
                                    'mesh':  {'nx': 4}})
    expected_str = 'append=False mesh.nx=4 '
    assert run_parameters.run_parameters_str == expected_str
    with pytest.raises(AttributeError):
        run_parameters.run_parameters_str = 'foo'
