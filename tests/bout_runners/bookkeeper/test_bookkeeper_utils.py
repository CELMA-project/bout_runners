from bout_runners.bookkeeper.bookkeeper_utils import \
    obtain_project_parameters
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_system_info_as_sql_type


def test_obtain_project_parameters(get_test_data_path):
    settings_path = get_test_data_path.joinpath('BOUT.settings')
    parameter_dict = obtain_project_parameters(settings_path)
    assert isinstance(parameter_dict, dict)
    assert 'global' in parameter_dict.keys()
    assert isinstance(parameter_dict['global'], dict)
    assert parameter_dict['global']['append'] == 'INTEGER'


def test_get_system_info_as_sql_type():
    sys_info_dict = get_system_info_as_sql_type()
    assert isinstance(sys_info_dict, dict)
