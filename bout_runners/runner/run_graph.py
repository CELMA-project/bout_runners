"""Contains the RunGraph class."""


import logging
from typing import Optional, Callable, Tuple, Any, Dict, Iterable, Union
import networkx as nx


class RunGraph:
    """
    A directed acyclic graph where the nodes contains instructions for execution.

    Attributes
    ----------
    __graph : nx.DiGraph
        The run graph
    __node_set : set
        The set of nodes belonging to the graph
    node :

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

    def __init__(self) -> None:
        """Instantiate the graph."""
        self.__graph = nx.DiGraph()
        self.__node_set = set(self.__graph.nodes)

    @property
    def nodes(self) -> nx.classes.reportviews.NodeView:
        """Return the nodes."""
        # NOTE: The set of nodes only contain the name of the nodes, not their
        #       attributes
        return self.__graph.nodes

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
        if name in self.__node_set:
            logging.warning(
                "'%s' is already present in the graph, and will be overwritten", name
            )
        self.__graph.add_node(name, function=function, args=args, kwargs=kwargs)
        self.__node_set = set(self.__graph.nodes)

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
        self.__node_set = set(self.__graph.nodes)
        return tuple(root_nodes)
