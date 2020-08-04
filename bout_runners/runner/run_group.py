"""Contains the RunGroup class."""

from typing import Optional, Union, Iterable, List, Callable, Tuple, Any, Dict

from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.runner.run_graph import RunGraph


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
