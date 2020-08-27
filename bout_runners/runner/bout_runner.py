"""Contains the BOUT runner class."""


import logging
from pathlib import Path
from typing import Optional, Dict, Callable, Tuple, Any

from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.runner.run_group import RunGroup


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
    def run_bout_run(bout_run_setup: BoutRunSetup, force: bool = False) -> None:
        """
        Perform the BOUT++ run and capture data.

        Parameters
        ----------
        bout_run_setup : BoutRunSetup
            The setup for the BOUT++ run
        force : bool
            Execute the run even if has been performed with the same parameters
        """
        run_id = bout_run_setup.metadata_recorder.capture_new_data_from_run(
            bout_run_setup.executor.submitter.processor_split, force
        )

        if run_id is None:
            logging.info("Executing the run")
            bout_run_setup.executor.execute()
        else:
            logging.warning(
                "Run with the same configuration has been executed before, "
                "see run with run_id %d",
                run_id,
            )
            if force:
                logging.info("Executing the run as force==True")
                bout_run_setup.executor.execute()

    @staticmethod
    def run_function(
        path: Path,
        function: Callable,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Execute the function from the node.

        # FIXME: You are here: Add submitter to the node

        Parameters
        ----------
        path : Path
            Absolute path to store the python file which holds the function and
            its arguments
        function : function
            The function to call
        args : tuple
            The positional arguments
        kwargs : dict
            The keyword arguments
        """
        logging.info(
            "Calling %s, with positional parameters %s, and keyword parameters %s",
            function.__name__,
            args,
            kwargs,
        )
        if args is None and kwargs is None:
            function()
        elif args is not None and kwargs is None:
            function(*args)
        elif args is None and kwargs is not None:
            function(**kwargs)
        elif args is not None and kwargs is not None:
            function(*args, **kwargs)

    def reset(self) -> None:
        """Reset the run_graph."""
        self.__run_graph.reset()

    def run(self, force: bool = False) -> None:
        """
        Execute all the nodes in the run_graph.

        Parameters
        ----------
        force : bool
            Execute the run even if has been performed with the same parameters

        Raises
        ------
        RuntimeError
            If none of the nodes in the `run_graph` has status "ready"
        """
        if force:
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
            for node in nodes_at_current_order.keys():
                logging.info("Executing %s", node)
                if node.startswith("bout_run"):
                    self.run_bout_run(
                        nodes_at_current_order[node]["bout_run_setup"], force
                    )
                else:
                    function = nodes_at_current_order[node]["function"]
                    args = nodes_at_current_order[node]["args"]
                    kwargs = nodes_at_current_order[node]["kwargs"]
                    path = nodes_at_current_order[node]["path"]
                    self.run_function(function, args, kwargs, path)
