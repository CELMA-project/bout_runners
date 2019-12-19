"""Module containing utils only used in the runners package."""


from bout_runners.runners.base_runner import BoutRunner


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
    runner = BoutRunner(execute_from_path=project_path)
    runner.set_inp_src(bout_inp_dir)
    runner.set_destination('test_run')

    settings_path = runner.destination.joinpath('BOUT.settings')

    if not settings_path.is_file():
        runner.set_split(1)
        runner.set_options({'global': {'nout': 0}})
        runner.run()

    return settings_path
