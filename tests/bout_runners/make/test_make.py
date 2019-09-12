import pytest
from distutils.dir_util import copy_tree
from distutils.dir_util import remove_tree
from bout_runners.make.make import MakeProject


@pytest.fixture(scope='function')
def make_make_object(get_bout_path):
    """
    Setup and teardown sequence which makes a make object

    The method calls make_obj.run_clean() before and after the yield
    statement

    Yields
    ------
    make_obj : MakeProject
        The object to call make and make clean from
    exec_file : Path
        The path to the executable
    """
    # Setup
    bout_path = get_bout_path
    project_path = bout_path.joinpath('examples', 'conduction')
    tmp_path = project_path.parent.joinpath('tmp')

    copy_tree(str(project_path), str(tmp_path))

    exec_file = tmp_path.joinpath('conduction')

    make_obj = MakeProject(makefile_root_path=tmp_path)
    make_obj.run_clean()

    yield make_obj, exec_file

    # Teardown
    remove_tree(str(tmp_path))


def test_make_project(make_make_object):
    """
    Tests that the MakeProject class is able to make conduction
    """

    make_obj, exec_file = make_make_object

    # NOTE: The setup runs make clean, so the project directory
    #       should not contain any executable
    assert not exec_file.is_file()

    make_obj.run_make()

    assert exec_file.is_file()

    # Check that the file is not made again
    # For detail, see
    # https://stackoverflow.com/a/52858040/2786884
    first_creation_time = exec_file.stat().st_ctime

    make_obj.run_make()

    assert first_creation_time == exec_file.stat().st_ctime

    # Check that the force flag makes the project again
    make_obj.run_make(force=True)

    assert first_creation_time != exec_file.stat().st_ctime
