"""Contains the submitter factory."""


import logging

from bout_runners.submitter.local_submitter import AbstractSubmitter
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.submitter.pbs_submitter import PBSSubmitter


def get_submitter(name: str, *args, **kwargs) -> AbstractSubmitter:
    """
    Return a Submitter object.

    Parameters
    ----------
    name : str
        Name of the submitter to use
    args : tuple
        Positional arguments (see the different implementation for details)
    kwargs : dict
        Keyword arguments (see the different implementation for details)

    Returns
    -------
    submitter : AbstractSubmitter
        The implemented submitter class

    Raises
    ------
    NotImplementedError
        If the name is not a supported submitter class
    """
    implemented = ("local", "pbs")

    logging.debug("Selecting a %s submitter", name)

    if name == "local":
        return LocalSubmitter(**kwargs)
    if name == "pbs":
        return PBSSubmitter(*args, **kwargs)

    msg = f"{name} is not a valid submitter class, choose " f"from {implemented}"
    raise NotImplementedError(msg)
