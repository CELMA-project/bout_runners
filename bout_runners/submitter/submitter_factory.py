"""Contains the submitter factory."""


import logging

from bout_runners.submitter.local_submitter import LocalSubmitter


def get_submitter(name: str, *args, **kwargs) -> LocalSubmitter:
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
    implemented = ("local",)

    if name == "local":
        submitter = LocalSubmitter(*args, **kwargs)
    else:
        msg = f"{name} is not a valid submitter class, choose " f"from {implemented}"
        raise NotImplementedError(msg)

    logging.debug("%s submitter selected", name)

    return submitter
