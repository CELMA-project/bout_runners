"""Module containing functions related to subprocess calls."""

import subprocess
import logging
from pathlib import Path
logger = logging.getLogger(__name__)


def run_subprocess(command, path):
    """
    Run a subprocess

    Parameters
    ----------
    command : str
        The command to run
    path : Path or str
        Path to the location to run the command from
    """

    logger.info(f'Executing "{command}" in {path}')
    result = subprocess.run(command.split(),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            cwd=path)

    if result.returncode != 0:
        raise_subprocess_error(result)


def raise_subprocess_error(result):
    """
    Raises errors from the subprocess in a clean way

    Parameters
    ----------
    result : subprocess.CompletedProcess
        The result from the subprocess
    """

    logger.error('Subprocess failed with stdout:')
    logger.error(result.stdout)
    logger.error('and stderr:')
    logger.error(result.stderr)

    result.check_returncode()


