import os
import pytest
from pathlib import Path
from bout_runners.utils.make import MakeProject
from dotenv import load_dotenv


@pytest.fixture(scope='function')
def make_make_object():
    """
    Setup and teardown sequence which makes a make object

    The method calls make_obj.run_clean() before and after the yield
    statement

    Yields
    ------
    make_obj : MakeProject
        The object to call make and make clean from
    """
    # Setup
    load_dotenv()
    bout_path = Path(os.getenv('BOUT_PATH')).absolute()
    project_path = bout_path.joinpath('examples', 'conduction')
    exec_file = project_path.joinpath('conduction')

    make_obj = MakeProject(makefile_root_path=project_path)
    make_obj.run_clean()

    yield make_obj, exec_file

    # Teardown
    make_obj.run_clean()


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
