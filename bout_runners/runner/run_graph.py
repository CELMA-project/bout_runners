"""Contains the RunGraph class."""


import logging
from typing import Optional, Callable, Tuple, Any, Dict, Iterable, Union
import networkx as nx
from bout_runners.runner.bout_run_setup import BoutRunSetup


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
    add_function_node(name, function=None, args=None, kwargs=None)
        Add a node to the graph
    add_edge(start_node, end_node)
        Connect two nodes through an directed edge
    add_waiting_for(nodes_to_wait_for, name_of_waiting_node)
        Make a node wait for the completion of one or more nodes
    pick_root()
        Picks and removes the root nodes from graph
    create_run_group(self, bout_run_setup, name, nodes_to_wait_for)
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

    def add_bout_run_node(self, name: str, bout_run_setup: BoutRunSetup,) -> None:
        """
        Add a node where the setup of a BOUT++ run is attached.

        FIXME: Test me

        Parameters
        ----------
        name : str
            Name of the node
        bout_run_setup : BoutRunSetup
            The setup of the BOUT++ run
        """
        if name in self.__node_set:
            logging.warning(
                "'%s' is already present in the graph, and will be overwritten", name
            )
        self.__graph.add_node(name, bout_run_setup=bout_run_setup)
        self.__node_set = set(self.__graph.nodes)

    def add_function_node(
        self,
        name: str,
        function: Optional[Callable] = None,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Add a node with an optionally attached callable to the graph.

        Parameters
        ----------
        name : str
            Name of the node
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

    def add_edge(self, start_node: str, end_node: str) -> None:
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
        name_of_waiting_node: str,
        nodes_to_wait_for: Optional[Union[str, Iterable[str]]],
    ) -> None:
        """
        Make a node wait for the completion of one or more nodes.

        In other words we will let one or more nodes point to name_of_waiting_node.

        Parameters
        ----------
        name_of_waiting_node : str
            Name of the node which will wait for the node(s) in waiting_for to finish
        nodes_to_wait_for : str or iterable
            Name of nodes the name_of_waiting_node will wait for
        """
        if nodes_to_wait_for is not None:
            if hasattr(nodes_to_wait_for, "__iter__") and not isinstance(
                nodes_to_wait_for, str
            ):
                for waiting_for_node in nodes_to_wait_for:
                    self.add_edge(waiting_for_node, name_of_waiting_node)
            elif isinstance(nodes_to_wait_for, str):
                self.add_edge(nodes_to_wait_for, name_of_waiting_node)

    def get_waiting_for_tuple(self, start_node_name) -> tuple:
        """
        Return the list of nodes waiting for a given node.

        Parameters
        ----------
        start_node_name : str
            Name of the node other nodes are waiting for

        Returns
        -------
        tuple
            Tuple of the nodes which are waiting for the given node
        """
        return tuple(nx.dfs_tree(self.__graph, start_node_name))

    def remove_node_with_dependencies(self, start_node_name) -> None:
        """
        Remove node and all nodes waiting for the specified node.

        Parameters
        ----------
        start_node_name : str
            Name of the node to remove
            All nodes waiting for start_node_name will be removed
        """
        nodes_to_remove = self.get_waiting_for_tuple(start_node_name)
        for node in nodes_to_remove:
            self.__graph.remove_node(node)

    def pick_root_nodes(self) -> tuple:
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

    def get_dot_string(self) -> str:
        """
        Return the graph as a string i the dot format.

        This can be visualized through GraphViz or online at http://www.webgraphviz.com/

        Returns
        -------
        str
            The graph written in the dot format
        """
        return str(nx.nx_pydot.to_pydot(self.__graph))
