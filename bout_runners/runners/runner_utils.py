"""Module containing utils only used in the runners package."""


import shutil
from bout_runners.runners.base_runner import single_run


def run_test_run(project_path, bout_inp_dir=None):
    """
    Perform a test run.

    Parameters
    ----------
    project_path : Path
        Path to the project
    bout_inp_dir : Path or None
        Path to the BOUT.inp file
        Will be set to `data/` of the `project_path` if not set

    Returns
    -------
    settings_path : Path
        Path to the settings file
    """
    if bout_inp_dir is None:
        bout_inp_dir = project_path.joinpath('data')

    test_run_dir = project_path.joinpath('test_run')
    if not test_run_dir.is_dir():
        test_run_dir.mkdir(exist_ok=True, parents=True)

    settings_path = test_run_dir.joinpath('BOUT.settings')

    if not settings_path.is_file():
        test_run_inp_path = test_run_dir.joinpath('BOUT.inp')
        bout_inp_path = bout_inp_dir.joinpath('BOUT.inp')
        shutil.copy(bout_inp_path, test_run_inp_path)

        single_run(execute_from_path=project_path,
                   bout_inp_dir=test_run_dir,
                   nproc=1,
                   options='nout=0')

    return settings_path
