"""Contains unittests for the bookkeeper utils."""

from bout_runners.bookkeeper.bookkeeper_utils import \
    obtain_project_parameters
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_system_info_as_sql_type
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_create_table_statement


def test_obtain_project_parameters(get_test_data_path):
    """
    Test that obtain_project_parameters returns expected output.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data
    """
    settings_path = get_test_data_path.joinpath('BOUT.settings')
    parameter_dict = obtain_project_parameters(settings_path)
    assert isinstance(parameter_dict, dict)
    assert 'global' in parameter_dict.keys()
    assert isinstance(parameter_dict['global'], dict)
    assert parameter_dict['global']['append'] == 'INTEGER'


def test_get_system_info_as_sql_type():
    """Test that the system info can be returned as a dict."""
    sys_info_dict = get_system_info_as_sql_type()
    assert isinstance(sys_info_dict, dict)


def test_get_create_table_statement():
    """Test that get_create_table_statement returns expected output."""
    result = get_create_table_statement(
        table_name='foo',
        columns=dict(bar='baz',
                     foobar='qux'),
        primary_key='quux',
        foreign_keys=dict(quuz=('corge', 'grault'),
                          garply=('waldo', 'fred')))

    expected = ('CREATE TABLE foo \n'
                '(   quux INTEGER PRIMARY KEY,\n'
                '    bar baz,\n'
                '    foobar qux,\n'
                '    quuz INTEGER,\n'
                '    garply INTEGER,\n'
                '    FOREIGN KEY(quuz) \n'
                '        REFERENCES corge(grault),\n'
                '    FOREIGN KEY(garply) \n'
                '        REFERENCES waldo(fred))')

    assert result == expected
