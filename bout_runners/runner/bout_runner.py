"""Contains the BOUT runner class."""


import re
import logging
import shutil
from pathlib import Path
from typing import Optional, Dict, Callable, Tuple, Any

from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.runner.run_group import RunGroup
from bout_runners.submitter.abstract_submitter import AbstractSubmitter
from bout_runners.submitter.local_submitter import LocalSubmitter


class BoutRunner:
    r"""
    Class for executing a run and store its metadata.

    Attributes
    ----------
    self.__executor : Executor
        Getter variable for executor
    self.__db_connector : DatabaseConnector
        Getter variable for db_connector
    self.__final_parameters : FinalParameters
        Getter variable for final_parameters
    self.__db_creator : DatabaseCreator
        Object used to create the database
    self.__metadata_recorder : MetadataRecorder
        Object used to record the metadata about a run
    self.executor : Executor
        Object used to execute the run
    self.db_creator : DatabaseCreator
        Object used to create the database
    self.final_parameters : FinalParameters
        Object containing the parameters to use

    Methods
    -------
    create_schema()
        Create the schema
    run(force)
        Execute the run

    Examples
    --------
    The easiest way to use BoutRunner is to run a script from the root directory of
    the project (i.e. where the `Makefile` and `data` directory are normally
    situated. The script can simply call

    >>> BoutRunner().run()

    and `BoutRunner` takes care of the rest.

    A more elaborate example where all the dependency objects are built manually:

    Import dependencies

    >>> from pathlib import Path
    >>> from bout_runners.executor.bout_paths import BoutPaths
    >>> from bout_runners.executor.executor import Executor
    >>> from bout_runners.database.database_connector import DatabaseConnector
    >>> from bout_runners.parameters.default_parameters import DefaultParameters
    >>> from bout_runners.parameters.run_parameters import RunParameters
    >>> from bout_runners.parameters.final_parameters import FinalParameters
    >>> from bout_runners.submitter.local_submitter import LocalSubmitter

    Create the `bout_paths` object

    >>> project_path = Path().joinpath('path', 'to', 'project')
    >>> bout_inp_src_dir = Path().joinpath('path', 'to', 'source', 'BOUT.inp')
    >>> bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination', 'BOUT.inp')
    >>> bout_paths = BoutPaths(project_path=project_path,
    ...                        bout_inp_src_dir=bout_inp_src_dir,
    ...                        bout_inp_dst_dir=bout_inp_dst_dir)

    Create the input objects

    >>> default_parameters = DefaultParameters(bout_paths)
    >>> run_parameters = RunParameters({'global': {'nout': 0}})
    >>> final_parameters = FinalParameters(default_parameters,
    ...                                    run_parameters)
    >>> executor = Executor(
    ...     bout_paths=bout_paths,
    ...     submitter=LocalSubmitter(bout_paths.project_path),
    ...     run_parameters=run_parameters)
    >>> db_connection = DatabaseConnector('name_of_database', db_root_path=Path())

    Run the project

    >>> runner = BoutRunner(executor,
    ...                     db_connection,
    ...                     final_parameters)
    >>> runner.run()
    """

    def __init__(self, run_graph: Optional[RunGraph] = None) -> None:
        """
        Set the member data.

        Parameters
        ----------
        run_graph : None or RunGraph
            The run graph to be executed
            If None the run graph will be constructed and added parameters from the
            default BoutRunSetup
        """
        if run_graph is None:
            self.__run_graph = RunGraph()
            _ = RunGroup(self.__run_graph, BoutRunSetup())
        else:
            self.__run_graph = run_graph
            if (
                len(
                    [
                        node
                        for node in self.__run_graph.nodes
                        if node.startswith("bout_run")
                    ]
                )
                == 0
            ):
                logging.warning("The provided run_graph does not contain any bout_runs")

    @property
    def run_graph(self) -> RunGraph:
        """
        Get the properties of self.run_graph.

        Returns
        -------
        self.__run_graph : RunGraph
            The RunGraph object
        """
        return self.__run_graph

    @staticmethod
    def run_bout_run(
        bout_run_setup: BoutRunSetup,
        restart_from_bout_inp_dst: bool = False,
        force: bool = False,
    ) -> Optional[int]:
        """
        Perform the BOUT++ run and capture data.

        Parameters
        ----------
        bout_run_setup : BoutRunSetup
            The setup for the BOUT++ run
        restart_from_bout_inp_dst : bool
            Restarts the run from the dump directory
            (bout_run_setup.executor.bout_paths.bout_inp_dst_dir)
            Note that it is also possible to specify the directory to restart from in
            executor.restart_from
            If True it will have precedence over anything specified in
            executor.restart_from
        force : bool
            Execute the run even if has been performed with the same parameters

        Returns
        -------
        pid : None or int
            The process id if the run is not skipped
        """
        if (
            restart_from_bout_inp_dst
            and bout_run_setup.executor.restart_from is not None
        ):
            logging.warning(
                "Both restart_from_bout_inp_dst and "
                "bout_run_setup.executor.restart_from specified. "
                "Using restart_from_bout_inp_dst"
            )

        if restart_from_bout_inp_dst:
            bout_run_setup.executor.restart_from = (
                bout_run_setup.executor.bout_paths.bout_inp_dst_dir
            )

        if bout_run_setup.executor.restart_from is not None:
            # NOTE: bout_run_setup is changed inplace
            BoutRunner.__reset_bout_inp_dst_dir(bout_run_setup)
            restart = True
        else:
            restart = False

        if restart and force:
            logging.warning(
                "force has been set to True for a run which is to use restart files. "
                "Will therefore ignore force"
            )

        run_id = bout_run_setup.metadata_recorder.capture_new_data_from_run(
            bout_run_setup.executor.submitter.processor_split, restart, force
        )

        pid = None
        if run_id is None:
            if not restart:
                logging.info("Executing the run")
            else:
                BoutRunner.copy_restart_files(bout_run_setup)
                logging.info("Executing the run from restart files")
            pid = bout_run_setup.executor.execute(restart)
        elif force:
            logging.info("Executing the run as force==True")
            pid = bout_run_setup.executor.execute()
        else:
            logging.warning(
                "Run with the same configuration has been executed before, "
                "see run with run_id %d",
                run_id,
            )

        return pid

    @staticmethod
    def copy_restart_files(bout_run_setup: BoutRunSetup) -> None:
        """
        Copy the restart files.

        Parameters
        ----------
        bout_run_setup : BoutRunSetup
            The BoutRunSetup object

        Raises
        ------
        FileNotFoundError
            If no restart files are found in bout_run_setup.executor.restart_from
        """
        if bout_run_setup.executor.restart_from is not None:
            src_list = list(bout_run_setup.executor.restart_from.glob("BOUT.restart.*"))
            if len(src_list) == 0:
                msg = (
                    f"No restart files files found in "
                    f"{bout_run_setup.executor.restart_from}"
                )
                logging.error(msg)
                raise FileNotFoundError(msg)
            for src in src_list:
                dst = bout_run_setup.executor.bout_paths.bout_inp_dst_dir.joinpath(
                    src.name
                )
                shutil.copy(src, dst)
                logging.debug("Copied %s to %s", src, dst)

    @staticmethod
    def __reset_bout_inp_dst_dir(bout_run_setup: BoutRunSetup):
        """
        Reset the bout_inp_dst_dir (inplace) to reflect that this is a restart run.

        The new bout_inp_dst_dir will be the same as
        bout_run_setup.executor.restart_from with _restart_/d* appended
        /d* will be the next digit based on the number of other restart directories

        Parameters
        ----------
        bout_run_setup : BoutRunSetup
            BoutRunSetup where bout_run_setup.executor.bout_paths.bout_inp_dst_dir
            is going to be altered
        """
        if bout_run_setup.executor.restart_from is not None:
            restart_dir_parent = bout_run_setup.executor.restart_from.parent
            restart_dir_name = bout_run_setup.executor.restart_from.name
            restart_dirs = list(restart_dir_parent.glob(f"{restart_dir_name}*"))
            restart_number = 0
            restart_numbers = list()
            pattern = r"_restart_(\d)+$"
            for restart_dir in restart_dirs:
                match = re.search(pattern, restart_dir.name)
                if match is not None:
                    # NOTE: THe zeroth group is the matching string
                    restart_numbers.append(int(match.group(1)))
            if len(restart_numbers) != 0:
                restart_numbers.sort()
                restart_number = restart_numbers[-1] + 1
            prev_inp_dst_dir = bout_run_setup.executor.bout_paths.bout_inp_dst_dir
            stripped_restart_dir_name = re.sub(pattern, "", restart_dir_name)
            new_inp_dst_dir = restart_dir_parent.joinpath(
                f"{stripped_restart_dir_name}_restart_{restart_number}"
            )
            bout_run_setup.executor.bout_paths.bout_inp_dst_dir = new_inp_dst_dir
            logging.info(
                "bout_run_setup.executor.bout_paths.bout_inp_dst_dir set from %s to %s",
                prev_inp_dst_dir,
                new_inp_dst_dir,
            )

    @staticmethod
    def run_function(
        path: Path,
        function: Callable,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        submitter: Optional[AbstractSubmitter] = None,
    ) -> Optional[int]:
        """
        Submit the function from the node.

        Parameters
        ----------
        path : Path
            Absolute path to store the python file which holds the function and
            its arguments
        function : function
            The function to call
        args : None or tuple
            The positional arguments
        kwargs : None or dict
            The keyword arguments
        submitter : None or AbstractSubmitter
            The submitter to submit the function with
            Uses the default LocalSubmitter if None

        Returns
        -------
        pid : int
            The process id
        """
        logging.info(
            "Submitting %s, with positional parameters %s, and keyword parameters %s",
            function.__name__,
            args,
            kwargs,
        )
        submitter = submitter if submitter is not None else LocalSubmitter()
        submitter.write_python_script(path, function, args, kwargs)
        command = f"python3 {path}"
        submitter.submit_command(command)
        return submitter.pid

    def reset(self) -> None:
        """Reset the run_graph."""
        logging.info("Resetting the graph")
        self.__run_graph.reset()

    def run(self, restart_all: bool = False, force: bool = False) -> None:
        """
        Execute all the nodes in the run_graph.

        Parameters
        ----------
        restart_all : bool
            All the BOUT++ runs in the run graph will be restarted
        force : bool
            Execute the run even if has been performed with the same parameters

        Raises
        ------
        RuntimeError
            If none of the nodes in the `run_graph` has status "ready"

        # FIXME: You are here: Ensure that the nodes are completed before
        #        starting the next one
        """
        if force or restart_all:
            logging.debug(
                "Resetting the graph as %s == True", "force" if force else "restart_all"
            )
            self.reset()

        if len(self.__run_graph) == 0:
            if len(self.__run_graph.nodes) == 0:
                msg = "The 'run_graph' does not contain any nodes."
            else:
                msg = (
                    "None of the nodes in 'run_graph' has the status 'ready'. "
                    "Reset the 'run_graph' if you'd like to run the original graph"
                )
            logging.error(msg)
            raise RuntimeError(msg)

        for nodes_at_current_order in self.__run_graph:
            pid_dict = dict()
            for node_name in nodes_at_current_order.keys():
                logging.info("Executing %s", node_name)
                if node_name.startswith("bout_run"):
                    pid = self.run_bout_run(
                        nodes_at_current_order[node_name]["bout_run_setup"],
                        restart_all,
                        force,
                    )
                    # NOTE: pid can be None if the run has already been executed
                    pid_dict[node_name] = pid
                else:
                    function = nodes_at_current_order[node_name]["function"]
                    args = nodes_at_current_order[node_name]["args"]
                    kwargs = nodes_at_current_order[node_name]["kwargs"]
                    path = nodes_at_current_order[node_name]["path"]
                    pid = self.run_function(function, args, kwargs, path)
                    pid_dict[node_name] = pid
                logging.debug("Job submitted with pid=%s", pid)
