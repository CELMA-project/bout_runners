"""Module containing functions related to subprocess calls."""


import subprocess
import logging


def run_subprocess(command, path):
    """
    Run a subprocess.

    Parameters
    ----------
    command : str
        The command to run
    path : Path or str
        Path to the location to run the command from
    """
    logging.info('Executing %s in %s', command, path)
    result = subprocess.run(command.split(),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            cwd=path,
                            check=False)

    if result.returncode != 0:
        raise_subprocess_error(result)


def raise_subprocess_error(result):
    """
    Raise and error from the subprocess in a clean way.

    Parameters
    ----------
    result : subprocess.CompletedProcess
        The result from the subprocess
    """
    logging.error('Subprocess failed with stdout:')
    logging.error(result.stdout)
    logging.error('and stderr:')
    logging.error(result.stderr)

    result.check_returncode()
