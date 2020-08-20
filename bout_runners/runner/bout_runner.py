"""Contains the BOUT runner class."""


import logging
from typing import Optional, Dict, Union, Callable, Tuple, Any

from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.runner.run_group import RunGroup


class BoutRunner:
    r"""
    Class for executing a run and store its metadata.

    #FIXME: Redo all this

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
        executor : Executor or None
            Object executing the run
            If None, default parameters will be used
        db_connector : DatabaseConnector or None
            The connection to the database
            If None: Default database connector will be used
        final_parameters : FinalParameters or None
            The object containing the parameters which are going to be used in the run
            If None, default parameters will be used
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
    def __run_bout_run(bout_run_setup: BoutRunSetup, force: bool) -> None:
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
    def __run_function(
        function_node: Dict[
            str,
            Union[
                Optional[Callable], Optional[Tuple[Any, ...]], Optional[Dict[str, Any]]
            ],
        ]
    ) -> None:
        """
        Execute the function from the node.

        Parameters
        ----------
        function_node : dict
            A dict containing the function, the positional arguments and the keyword
            arguments
        """
        function = function_node["function"]
        args = function_node["args"]
        kwargs = function_node["kwargs"]
        function(*args, **kwargs)

    def run(self, force: bool = False) -> None:
        """
        Execute all the nodes in the run_graph.

        Parameters
        ----------
        force : bool
            Execute the run even if has been performed with the same parameters
        """
        while len(self.__run_graph.nodes) != 0:
            root_nodes = self.__run_graph.pick_root_nodes()
            # FIXME: YOU ARE HERE: You don't get the names here,
            #  only the nodes themselves
            for node in root_nodes:
                logging.info("Executing %s", node)
                if node.startswith("bout_run"):
                    self.__run_bout_run(
                        self.__run_graph.nodes[node]["bout_run_setup"], force
                    )
                else:
                    self.__run_function(self.__run_graph.nodes[node])
