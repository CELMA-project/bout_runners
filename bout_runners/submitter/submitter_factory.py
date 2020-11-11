"""Contains the submitter factory."""


import configparser
import logging
from typing import Any, Dict, Optional, Tuple

from bout_runners.submitter.local_submitter import AbstractSubmitter, LocalSubmitter
from bout_runners.submitter.pbs_submitter import PBSSubmitter
from bout_runners.submitter.processor_split import ProcessorSplit
from bout_runners.utils.paths import get_submitters_configuration


def get_submitter(
    name: Optional[str] = None,
    argument_dict: Optional[Dict[str, Any]] = None,
) -> AbstractSubmitter:
    """
    Return a Submitter object.

    Parameters
    ----------
    name : str or None
        Name of the submitter to use
        If None the submitter will be inferred
    argument_dict : dict
        Dict containing positional and keyword arguments

    Other Parameters
    ----------------
    The following parameters can be given argument_dict

    processor_split : ProcessorSplit or None
        Object containing the processor split
        Used for all submitters
    run_path : Path or str or None
        Positional argument
        Directory to run the command from
        Used in LocalSubmitters
    job_name : str or None
        Positional argument
        Name of the job
        Used for cluster submitters
    store_directory : path or None
        Keyword agrument
        Directory to store the scripts
        Used for cluster submitters
    submission_dict : None or dict of str of None or str
        Keyword agrument
        Dict containing optional submission options
        One the form

        >>> {'walltime': None or str,
        ...  'account': None or str,
        ...  'queue': None or str,
        ...  'mail': None or str}

        These options will not be used if the submission_dict is None
        Used for cluster submitters

    Returns
    -------
    submitter : AbstractSubmitter
        The implemented submitter class

    Raises
    ------
    ValueError
        If the input does not match the desired submitter
    NotImplementedError
        If the name is not a supported submitter class
    """
    implemented = ("local", "pbs")

    if name is None or argument_dict is None:
        name, argument_dict = infer_submitter()

    logging.debug("Choosing a %s submitter", name)

    if "processor_split" not in argument_dict.keys():
        argument_dict["processor_split"] = ProcessorSplit()
    if name == "local":
        if "run_path" not in argument_dict.keys():
            argument_dict["run_path"] = None
        return LocalSubmitter(
            run_path=argument_dict["run_path"],
            processor_split=argument_dict["processor_split"],
        )
    if name in ("pbs", "slurm"):
        for argument in ("job_name", "store_directory", "submission_dict"):
            if argument not in argument_dict.keys():
                argument_dict[argument] = None
    if name == "pbs":
        return PBSSubmitter(
            job_name=argument_dict["job_name"],
            store_directory=argument_dict["store_directory"],
            submission_dict=argument_dict["submission_dict"],
            processor_split=argument_dict["processor_split"],
        )

    msg = f"{name} is not a valid submitter class, choose " f"from {implemented}"
    logging.critical(msg)
    raise NotImplementedError(msg)


def infer_submitter() -> Tuple[str, Dict[str, Any]]:
    """
    Infer the submitter and return appropriate positional and keyword arguments.

    Returns
    -------
    name : str
        Name of the submitter type
    argument_dict : dict
        Dict containing positional and keyword arguments
    """
    submitter_config = get_submitters_configuration()
    slurm_available = slurm_is_available()
    pbs_available = pbs_is_available()
    if pbs_available or slurm_available:
        submission_dict = get_submission_dict(submitter_config["cluster"])
        processor_split = get_processor_split(submitter_config["cluster"])
        argument_dict = {
            "submission_dict": submission_dict,
            "processor_split": processor_split,
        }
        if slurm_available:
            logging.info("Inferred to use the SLURM submitter")
            name = "slurm"
        else:
            # Use PBS
            logging.info("Inferred to use the PBS submitter")
            name = "pbs"
    else:
        name = "local"
        # NOTE: We will always run one node for local submissions
        processor_split = get_processor_split(submitter_config["local"])
        # NOTE: As the keyword argument run_path defaults to the caller dir
        #       we will not override the option here, but let the constructor
        #       give a default value
        argument_dict = {"processor_split": processor_split}

    return name, argument_dict


def slurm_is_available() -> bool:
    """
    Check if the SLURM system is available.

    Returns
    -------
    slurm_available : bool
        True if SLURM is available
    """
    # Submit the command through a local submitter
    local_submitter = LocalSubmitter()
    try:
        local_submitter.submit_command("squeue")
        local_submitter.wait_until_completed(raise_error=False)
        slurm_available = not local_submitter.errored()
    except FileNotFoundError:
        # subprocess.Popen throws FileNotFoundError if a command is not in scope
        slurm_available = False

    logging.debug("SLURM is%s available", " not" if not slurm_available else "")
    return slurm_available


def pbs_is_available() -> bool:
    """
    Check if the PBS system is available.

    Returns
    -------
    pbs_available : bool
        True if PBS is available
    """
    # Submit the command through a local submitter
    local_submitter = LocalSubmitter()
    try:
        local_submitter.submit_command("qstat")
        local_submitter.wait_until_completed(raise_error=False)
        pbs_available = not local_submitter.errored()
    except FileNotFoundError:
        # subprocess.Popen throws FileNotFoundError if a command is not in scope
        pbs_available = False

    logging.debug("PBS is%s available", " not" if not pbs_available else "")
    return pbs_available


def get_submission_dict(
    cluster_section: configparser.SectionProxy,
) -> Dict[str, Optional[str]]:
    """
    Return the submission dict based on the submitter configuration file.

    Parameters
    ----------
    cluster_section : configparser.SectionProxy
        The cluster section of the submitters configuration
        Must contain the sections walltime, account, queue, mail

    Returns
    -------
    submission_dict : dict
        Dictionary containing options for the submission_dict
        The submission_dict is a keyword parameter to the cluster constructors

    See Also
    --------
    AbstractClusterSubmitter : Submitter object used for clusters
    """
    submission_dict: Dict[str, Optional[str]] = dict()
    for key in ("walltime", "account", "queue", "mail"):
        submission_dict[key] = (
            None if cluster_section[key].lower() == "none" else cluster_section[key]
        )
    return submission_dict


def get_processor_split(submitter_section: configparser.SectionProxy) -> ProcessorSplit:
    """
    Return the processor split based on the submitter configuration file.

    Parameters
    ----------
    submitter_section : configparser.SectionProxy
        The section of the submitters configuration to use
        Must contain the section number_of_processors

    Returns
    -------
    ProcessorSplit
        The processor split object
    """
    number_of_processors = int(submitter_section["number_of_processors"])

    number_of_nodes = (
        1
        if "number_of_nodes" not in submitter_section
        else int(submitter_section["number_of_nodes"])
    )
    processors_per_node = (
        1
        if "processors_per_node" not in submitter_section
        else int(submitter_section["processors_per_node"])
    )

    return ProcessorSplit(
        number_of_processors=number_of_processors,
        number_of_nodes=number_of_nodes,
        processors_per_node=processors_per_node,
    )
