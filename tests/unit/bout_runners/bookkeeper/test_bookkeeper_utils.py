"""Contains unittests for the bookkeeper utils."""

from bout_runners.bookkeeper.bookkeeper_utils import \
    obtain_project_parameters
from bout_runners.bookkeeper.bookkeeper_utils import \
    cast_parameters_to_sql_type
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_system_info_as_sql_type
from bout_runners.bookkeeper.bookkeeper_utils import \
    get_create_table_statement


def test_cast_parameters_to_sql_type(get_test_data_path):
    """
    Test that obtain_project_parameters returns expected output.

    Parameters
    ----------
    get_test_data_path : Path
        Path to the test data
    """
    settings_path = get_test_data_path.joinpath('BOUT.settings')
    parameter_dict = obtain_project_parameters(settings_path)
    parameter_as_sql = cast_parameters_to_sql_type(parameter_dict)
    assert isinstance(parameter_as_sql, dict)
    assert 'global' in parameter_as_sql.keys()
    assert isinstance(parameter_as_sql['global'], dict)
    assert parameter_as_sql['global']['append'] == 'TEXT'


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
                '    bar baz NOT NULL,\n'
                '    foobar qux NOT NULL,\n'
                '    quuz INTEGER NOT NULL,\n'
                '    garply INTEGER NOT NULL,\n'
                '    FOREIGN KEY(quuz) \n'
                '        REFERENCES corge(grault)\n'
                '            ON UPDATE CASCADE\n'
                '            ON DELETE CASCADE,\n'
                '    FOREIGN KEY(garply) \n'
                '        REFERENCES waldo(fred)\n'
                '            ON UPDATE CASCADE\n'
                '            ON DELETE CASCADE)')

    assert result == expected
