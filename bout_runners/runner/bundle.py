"""Contains the bundle class."""


import logging
from typing import Optional, Callable, Tuple, List, Any, Dict, Iterable, Union
import networkx as nx

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.executor.executor import Executor
from bout_runners.metadata.metadata_recorder import MetadataRecorder
from bout_runners.parameters.final_parameters import FinalParameters


class BoutRunSetup:
    """
    Class for setting up the BOUT++ run.

    More specifically this class will connect the executor object with the run
    parameters and a database to store the results in

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
    __create_schema()
        Create the schema

    Examples
    --------
    # FIXME: Can the BOUT++ run be formulated as a function? Setup?
    # Answer: The function which calls the run is executor.execute

    # submission_pid[object_id] = pid
    # FIXME: Find the inverse of this dict as well
    # FIXME: String representation
    # FIXME: Possibility to add different submissions

    # FIXME: Add examples
    # FIXME: Split files
    # FIXME: Make documentation in readthedocs
    # FIXME: Enable parallel execution on single machine, user can specify max nodes
    # FIXME: Add monitor: Execute the next in line when pid has finished. If not
    #        success -> broken chain, but the rest can continue
    # multiprocessing.Queue([maxsize])
    # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Queue

    >>> run_setup = BoutRunSetup(executor, db_connector, final_parameters)
    >>> run_graph = RunGraph()
    >>> run_graph.create_run_group(run_setup)
    >>> runner = BoutRunner(run_graph)
    >>> runner.run()
    """

    def __init__(
        self,
        executor: Optional[Executor] = None,
        db_connector: Optional[DatabaseConnector] = None,
        final_parameters: Optional[FinalParameters] = None,
    ) -> None:
        """
        Set the member data.

        This constructor will also create the schema if it does not exist.

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
        # Set member data
        # NOTE: We are not setting the default as a keyword argument
        #       as this would mess up the paths
        self.__executor = executor if executor is not None else Executor()
        self.__final_parameters = (
            final_parameters if final_parameters is not None else FinalParameters()
        )
        self.__db_connector = (
            db_connector if db_connector is not None else DatabaseConnector()
        )
        self.__db_creator = DatabaseCreator(self.db_connector)
        self.__metadata_recorder = MetadataRecorder(
            self.__db_connector, self.executor.bout_paths, self.final_parameters
        )

        if not self.__metadata_recorder.db_reader.check_tables_created():
            logging.info(
                "Creating schema as no tables were found in " "%s",
                self.__metadata_recorder.db_reader.db_connector.db_path,
            )
            self.__create_schema()

    @property
    def executor(self) -> Executor:
        """
        Get the properties of self.executor.

        Returns
        -------
        self.__executor : Executor
            The executor object
        """
        return self.__executor

    @property
    def final_parameters(self) -> FinalParameters:
        """
        Get the properties of self.final_parameters.

        Returns
        -------
        self.__final_parameters : FinalParameters
            The object containing the parameters used in the run
        """
        return self.__final_parameters

    @property
    def db_connector(self) -> DatabaseConnector:
        """
        Get the properties of self.db_connector.

        Returns
        -------
        self.__db_connector : DatabaseConnector
            The object holding the database connection
        """
        return self.__db_connector

    def __create_schema(self) -> None:
        """Create the schema."""
        final_parameters_dict = self.final_parameters.get_final_parameters()
        final_parameters_as_sql_types = self.final_parameters.cast_to_sql_type(
            final_parameters_dict
        )
        self.__db_creator.create_all_schema_tables(final_parameters_as_sql_types)


class RunGraph:
    """
    A directed acyclic graph where the nodes contains instructions for execution.

    Attributes
    ----------
    __graph : nx.DiGraph
        The run graph
    __nodes : set
        The set of nodes belonging to the graph

    Methods
    -------
    add_node(name, function=None, args=None, kwargs=None)
        Add a node to the graph
    add_edge(start_node, end_node)
        Connect two nodes through an directed edge
    add_waiting_for(waiting_for, name_of_waiting_node)
        Make a node wait for the completion of one or more nodes
    pick_root()
        Picks and removes the root nodes from graph
    create_run_group(self, bout_run_setup, name, waiting_for)
        Create a run group attached to the run graph

    See Also
    --------
    RunGroup : Class for building a run group
    """

    def __init__(self):
        """Instantiate the graph."""
        self.__graph = nx.DiGraph()
        self.__nodes = set(self.__graph.nodes)

    def add_node(
        self,
        name: str,
        function: Optional[Callable] = None,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Add a node to the graph.

        Parameters
        ----------
        name : str
            Name of the node (must be unique)
        function : None or callable
            The function to be called
            Will be None in the case of the bout_run_setup
        args : None or tuple
            Optional arguments to the function
        kwargs : None or dict
            Optional keyword arguments to the function
        """
        if name in self.__nodes:
            logging.warning(
                "%s is already present in the graph, and will be overwritten", name
            )
        self.__graph.add_node(name, function=function, args=args, kwargs=kwargs)
        self.__nodes = set(self.__graph.nodes)

    def add_edge(self, start_node: str, end_node: str):
        """
        Connect two nodes through an directed edge.

        Parameters
        ----------
        start_node : str
            Name of the start node
        end_node : str
            Name of the end node

        Raises
        ------
        ValueError
            If the graph after adding the nodes becomes cyclic
        """
        self.__graph.add_edge(start_node, end_node)
        if not nx.is_directed_acyclic_graph(self.__graph):
            raise ValueError(
                f"The node connection from {start_node} to {end_node} "
                f"resulted in a cyclic graph"
            )

    def add_waiting_for(
        self,
        waiting_for: Optional[Union[str, Iterable[str]]],
        name_of_waiting_node: str,
    ):
        """
        Make a node wait for the completion of one or more nodes.

        In other words we will let one or more nodes point to name_of_waiting_node.

        Parameters
        ----------
        waiting_for : str or iterable
            Name of nodes the name_of_waiting_node will wait for
        name_of_waiting_node : str
            Name of the node which will wait for the node(s) in waiting_for
        """
        if waiting_for is not None:
            if hasattr(waiting_for, "__iter__") and not isinstance(waiting_for, str):
                for waiting_for_node in waiting_for:
                    self.add_edge(waiting_for_node, name_of_waiting_node)
            elif isinstance(waiting_for, str):
                self.add_edge(waiting_for, name_of_waiting_node)

    def pick_root_nodes(self):
        """
        Pick and remove the root nodes from graph.

        Returns
        -------
        root_nodes : tuple of dict
            Tuple of the attributes of the nodes
        """
        roots = tuple(node for node, degree in self.__graph.in_degree() if degree == 0)
        root_nodes = list()
        for root in roots:
            root_nodes.append(self.__graph.nodes[root])
            self.__graph.remove_node(root)
        self.__nodes = set(self.__graph.nodes)
        return tuple(root_nodes)

    def create_run_group(
        self,
        bout_run_setup: BoutRunSetup,
        name: Optional[str] = None,
        waiting_for: Union[str, Iterable[str]] = None,
    ):
        """
        Create a run group attached to the run graph.

        Parameters
        ----------
        bout_run_setup : BoutRunSetup
            The setup for the BOUT++ run
        name : None or str
            Name of the RunGroup
            If None, a unique number will be assigned
        waiting_for : str or iterable
            Name of nodes the name_of_waiting_node will wait for

        Returns
        -------
        RunGroup
            The run group

        See Also
        --------
        RunGroup : Class for building a run group
        """
        return RunGroup(self, bout_run_setup, name, waiting_for)


class RunGroup:
    """
    Class for building a run group.

    A run group contains one recipe for executing the project (called bout_run_setup).
    The run group may consist of pre-processors (functions that will run prior to the
    bout_run_setup execution), and it may consist of post-processors (functions that
    will run after the bout_run_setup execution).

    Attributes
    ----------
    __counter : int
        Counter used if no name is given in the constructor
    __run_graph : RunGraph
        The RunGraph which the RunGroup is attached to
    __bout_run_setup : BoutRunSetup
        The setup of the BOUT++ run
    __pre_processors : list
        List of pre-processors (which will run before the BOUT++ run)
    __post_processors
        List of pre-processors (which will run after the BOUT++ run)

    Methods
    -------
    add_pre_processor(function, name, args, kwargs, waiting_for)
        Add a function which will run prior to the BOUT++ run
    add_post_processor(function, name, args, kwargs, waiting_for)
        Add a function which will run after the BOUT++ run
    """

    __counter = 0

    def __init__(
        self,
        run_graph: RunGraph,
        bout_run_setup: BoutRunSetup,
        name: Optional[str] = None,
        waiting_for: Optional[Union[str, Iterable[str]]] = None,
    ):
        """
        Set the member data.

        If you want to connect nodes to this RunGroup after creation, you can use
        RunGraph.add_node

        Parameters
        ----------
        run_graph : RunGraph
            The RunGraph which the RunGroup is attached to
        bout_run_setup : BoutRunSetup
            The setup of the BOUT++ run
        name : None or str
            Name of the RunGroup
            If None, the class counter will be used
        waiting_for : None or str or iterable
            Name of nodes the name_of_waiting_node will wait for
        """
        self.__run_graph = run_graph
        self.__bout_run_setup = bout_run_setup
        self.__name = name
        self.__pre_processors: List[str] = list()
        self.__post_processors: List[str] = list()

        if self.__name is None:
            self.__name = str(RunGroup.__counter)
            RunGroup.__counter += 1

        # Assign a node to bout_run_setup
        self.bout_run_node_name = f"bout_run_{self.__name}"
        self.__run_graph.add_node(self.bout_run_node_name)

        # Add edges to the nodes
        self.__run_graph.add_waiting_for(waiting_for, self.bout_run_node_name)

    def add_pre_processor(
        self,
        function: Callable,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        waiting_for: Optional[Union[str, Iterable[str]]] = None,
    ) -> None:
        """
        Add a pre-processor to the BOUT++ run.

        Parameters
        ----------
        function : callable
            The function to execute
        args : None or tuple
            Optional arguments to the function
        kwargs : None or dict
            Optional keyword arguments to the function
        waiting_for : None or str or iterable
            Name of nodes this node will wait for to finish before executing
        """
        pre_processor_node_name = (
            f"pre_processor_{self.__name}_{len(self.__pre_processors)}"
        )
        self.__run_graph.add_node(
            self.bout_run_node_name, function=function, args=args, kwargs=kwargs
        )
        self.__run_graph.add_edge(pre_processor_node_name, self.bout_run_node_name)
        self.__run_graph.add_waiting_for(waiting_for, pre_processor_node_name)
        self.__pre_processors.append(pre_processor_node_name)

    def add_post_processor(
        self,
        function: Callable,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        waiting_for: Optional[Union[str, Iterable[str]]] = None,
    ) -> None:
        """
        Add a post-processor to the BOUT++ run.

        Parameters
        ----------
        function : callable
            The function to execute
        args : None or tuple
            Optional arguments to the function
        kwargs : None or dict
            Optional keyword arguments to the function
        waiting_for : None or str or iterable
            Name of nodes this node will wait for to finish before executing
        """
        post_processor_node_name = (
            f"post_processor_{self.__name}_{len(self.__post_processors)}"
        )
        self.__run_graph.add_node(
            self.bout_run_node_name, function=function, args=args, kwargs=kwargs
        )
        self.__run_graph.add_edge(self.bout_run_node_name, post_processor_node_name)
        self.__run_graph.add_waiting_for(waiting_for, post_processor_node_name)
        self.__pre_processors.append(post_processor_node_name)
