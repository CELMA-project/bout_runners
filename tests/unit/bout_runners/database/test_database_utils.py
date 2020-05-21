"""Contains unittests for the database utils."""

from bout_runners.database.database_utils import get_system_info_as_sql_type


def test_get_system_info_as_sql_type():
    """Test that the system info can be returned as a dict."""
    sys_info_dict = get_system_info_as_sql_type()
    assert isinstance(sys_info_dict, dict)
