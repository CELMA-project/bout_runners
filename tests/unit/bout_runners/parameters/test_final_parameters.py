"""Contains unittests for final parameters."""

from bout_runners.parameters.final_parameters import FinalParameters
from bout_runners.parameters.run_parameters import RunParameters


def test_final_parameters(get_default_parameters):
    """
    Test that RunParameters overwrites DefaultParameters.

    Parameters
    ----------
    get_default_parameters : DefaultParameters
        The DefaultParameters object
    """
    default_parameters = get_default_parameters
    run_parameters = RunParameters({'global': {'timestep': False}})
    final_parameters = FinalParameters(default_parameters,
                                       run_parameters)

    final_parameters_dict = final_parameters.get_final_parameters()

    assert final_parameters_dict['global']['timestep'] is False
