"""Contains unittests for the creator."""

import pytest
from bout_runners.bookkeeper.creator import create_system_info_table
from bout_runners.bookkeeper.creator import create_run_table
from bout_runners.bookkeeper.creator import \
    create_file_modification_table
from bout_runners.bookkeeper.creator import create_parameter_tables
from bout_runners.bookkeeper.creator import create_split_table


@pytest.fixture(scope='module')
def make_creator_database(make_test_database):
    bk = make_test_database('creator.db')
    return bk


def test_create_run_table(make_creator_database):
    bk = make_creator_database
    create_run_table(bk)
    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bk.query(query_str)
    assert 'run' in table.loc[:, 'name'].values


def test_create_parameter_tables(make_creator_database, make_project):
    bk = make_creator_database
    project_path = make_project
    create_parameter_tables(bk, project_path)
    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bk.query(query_str)
    expected = set(['global',
                    'conduction',
                    'mesh',
                    'mesh_ddz',
                    'output',
                    'restart',
                    'solver',
                    't',
                    'all_boundaries',
                    'parameters'])
    assert expected.issubset(set(table.loc[:, 'name'].values))


def test_create_file_modification_table(make_creator_database):
    bk = make_creator_database
    create_file_modification_table(bk)
    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bk.query(query_str)
    assert 'file_modification' in table.loc[:, 'name'].values


def test_create_split_table(make_creator_database):
    bk = make_creator_database
    create_split_table(bk)
    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bk.query(query_str)
    assert 'split' in table.loc[:, 'name'].values


def test_create_system_info_table(make_creator_database):
    bk = make_creator_database
    create_system_info_table(bk)
    query_str = ('SELECT name FROM sqlite_master '
                 '   WHERE type="table"')
    table = bk.query(query_str)
    assert 'system_info' in table.loc[:, 'name'].values
