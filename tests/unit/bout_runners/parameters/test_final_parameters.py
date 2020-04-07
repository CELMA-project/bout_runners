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


def test_cast_parameters_to_sql_type(get_test_data_path,
                                     get_default_parameters):
    """
    Test that obtain_project_parameters returns expected output.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data
    get_default_parameters : DefaultParameters
        The DefaultParameters object
    """
    default_parameters = get_default_parameters
    final_parameters = FinalParameters(default_parameters)
    final_parameters_dict = final_parameters.get_final_parameters()
    parameter_as_sql = \
        final_parameters.\
        cast_parameters_to_sql_type(final_parameters_dict)
    assert isinstance(parameter_as_sql, dict)
    assert 'global' in parameter_as_sql.keys()
    assert isinstance(parameter_as_sql['global'], dict)
    assert parameter_as_sql['global']['append'] == 'TEXT'
