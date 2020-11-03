"""Contains the RunGraph class."""


import logging
from pathlib import Path
from typing import Optional, Callable, Tuple, Any, Dict, Iterable, Union
import networkx as nx
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.submitter.abstract_submitters import AbstractSubmitter
from bout_runners.submitter.local_submitter import LocalSubmitter


class RunGraph:
    """
    A directed acyclic graph where the nodes contains instructions for execution.

    Attributes
    ----------
    __graph : nx.DiGraph
        The run graph
    __node_set : set
        The set of nodes belonging to the graph
    nodes : nx.classes.reportviews.NodeView
        Return the nodes

    Methods
    -------
    __iter__()
        Make the class iterable
    __next__()
        Return the next order nodes from graph (ordered by the breadth)
    __len__()
        Return the number of nodes with status ready
    __getitem__(nodename)
        Return the content of a node
    __get_pruned_clone()
        Return a clone of the "ready" nodes of self.__graph
    add_bout_run_node(name, bout_run_setup)
        Add a node where the setup of a BOUT++ run is attached
    add_function_node(name, function_dict=None, path=None, submitter=None)
        Add a node with an optionally attached callable to the graph
    add_edge(start_node, end_node)
        Connect two nodes through an directed edge
    add_waiting_for(nodes_to_wait_for, name_of_waiting_node)
        Make a node wait for the completion of one or more nodes
    change_status_node_and_dependencies(start_node_name, status="errored")
        Remove node and all nodes waiting for the specified node
    get_waiting_for_tuple(start_node_name)
        Return the list of nodes waiting for a given node
    pick_root()
        Picks and removes the root nodes from graph
    reset()
        Reset the nodes by setting the status to 'ready'
    get_dot_string()
        Return the graph as a string i the dot format

    Examples
    --------
    The RunGraph contains the graph executed by BoutRunners

    >>> bout_run_setup = BoutRunSetup(executor, db_connector, final_parameters)
    >>> run_graph = RunGraph()
    >>> # Attach a RunGroup to the run_graph
    >>> _ = RunGroup(run_graph, bout_run_setup)
    >>> runner = BoutRunner(run_graph)
    >>> runner.run()

    See Also
    --------
    RunGroup : Class for building a run group
    """

    def __init__(self) -> None:
        """Instantiate the graph."""
        logging.info("Start: Making a RunGraph object")
        self.__graph = nx.DiGraph()
        self.__node_set = set(self.__graph.nodes)
        logging.info("Done: Making a RunGraph object")

    def __iter__(self) -> "RunGraph":
        """
        Make the class iterable.

        Returns
        -------
        self : RunGraph
            The class as an iterable
        """
        return self

    def __next__(self) -> Dict[str, Dict[str, Any]]:
        """
        Return the next order nodes from graph (ordered by the breadth).

        Raises
        ------
        StopIteration
            When the iteration is exhausted

        Returns
        -------
        nodes_at_current_order : dict of str, dict
            Dict of the attributes of the nodes
        """
        # NOTE: The while loop can be done more efficient with the walrus operator,
        #       but this will break compatibility with python < 3.8
        clone = self.__get_pruned_clone()
        if len(clone) != 0:
            # Find all roots of the clone
            # In degree is number of edges pointing to the node
            roots = tuple(node for node, degree in clone.in_degree() if degree == 0)

            nodes_at_current_order = dict()
            for root in roots:
                nodes_at_current_order[root] = self.__graph.nodes[root]
                self.__graph.nodes[root]["status"] = "traversed"
            return nodes_at_current_order

        raise StopIteration

    def __len__(self) -> int:
        """
        Return the number of nodes with status ready.

        Returns
        -------
        length : len
            Number of nodes with status ready.
        """
        length = 0
        for node_name in self.__graph:
            if self.__graph.nodes[node_name]["status"] == "ready":
                length += 1
        return length

    def __getitem__(self, node_name: str) -> Dict[str, Any]:
        """
        Return the content of a node.

        Parameters
        ----------
        node_name : str
            The name of the node

        Returns
        -------
        dict
            The node content
        """
        # It seems like this is producing a false positive
        return self.nodes[node_name]  # type: ignore

    def __get_pruned_clone(self) -> nx.DiGraph:
        """
        Return a clone of the "ready" nodes of self.__graph.

        Returns
        -------
        clone : nx.Digraph
            A clone of self.__graph where all nodes with another status than "ready"
            has been removed
        """
        clone = self.__graph.copy()
        for node_name in self.__graph:
            if self.__graph.nodes[node_name]["status"] != "ready":
                clone.remove_node(node_name)
        return clone

    @property
    def nodes(self) -> nx.classes.reportviews.NodeView:
        """Return the nodes."""
        # NOTE: The set of nodes only contain the name of the nodes, not their
        #       attributes
        return self.__graph.nodes

    def predecessors(self, node_name: str) -> Tuple[str, ...]:
        """
        Return the predecessors of the node.

        Parameters
        ----------
        node_name : str
            Name of the node to get the predecessors from

        Returns
        -------
        predecessor_names : tuple of str
            Names of predecessors
        """
        # NOTE: The set of nodes only contain the name of the nodes, not their
        #       attributes
        return tuple(self.__graph.predecessors(node_name))

    def successors(self, node_name: str) -> Tuple[str, ...]:
        """
        Return the successors of the node.

        Parameters
        ----------
        node_name : str
            Name of the node to get the predecessors from

        Returns
        -------
        successors_names : tuple of str
            Names of predecessors
        """
        # NOTE: The set of nodes only contain the name of the nodes, not their
        #       attributes
        return tuple(self.__graph.successors(node_name))

    def in_degree(self, node_name: str) -> int:
        """
        Return the number of edges pointing to the node.

        Parameters
        ----------
        node_name : str
            Name of the node to get the in_degree from

        Returns
        -------
        in_degree : int
            Number of edges pointing to the node
        """
        in_degree = self.__graph.in_degree(node_name)
        if not isinstance(in_degree, int):
            msg = f"The returned type from .in_degree was of type {type(in_degree)}, not int"
            logging.critical(msg)
            raise ValueError(msg)
        return in_degree

    def bfs_nodes(
        self, node_name: str, reverse_search: bool = False
    ) -> Tuple[str, ...]:
        """
        Return the nodes of a breadth first search.

        # FIXME: Make algorithm

        Parameters
        ----------
        node_name : str
            Name of the node to search from
        reverse_search : bool
            Whether to search reversed in breadth first search

        Returns
        -------
        nodes : tuple of str
            Nodes from the bfs
        """
        edges = nx.bfs_edges(self.__graph, node_name, reverse=reverse_search)
        nodes = [node_name] + [to_node for _, to_node in edges]
        return tuple(nodes)

    def reset(self) -> None:
        """Reset the nodes by setting status to 'ready' and calling node.reset()."""
        logging.info("Resetting the nodes")
        for node_name in self.__graph:
            self.__graph.nodes[node_name]["status"] = "ready"
            self.__graph.nodes[node_name]["submitter"].reset()

    def add_bout_run_node(
        self,
        name: str,
        bout_run_setup: BoutRunSetup,
    ) -> None:
        """
        Add a node where the setup of a BOUT++ run is attached.

        Parameters
        ----------
        name : str
            Name of the node
        bout_run_setup : BoutRunSetup
            The setup of the BOUT++ run

        Raises
        ------
        ValueError
            If the node is already present in the graph
        """
        if name in self.__node_set:
            raise ValueError(f"'{name}' is already present in the graph")

        self.__graph.add_node(
            name,
            bout_run_setup=bout_run_setup,
            submitter=bout_run_setup.submitter,
            status="ready",
        )

        self.__node_set = set(self.__graph.nodes)

    def add_function_node(
        self,
        name: str,
        function_dict: Optional[
            Dict[str, Optional[Union[Callable, Tuple[Any, ...], Dict[str, Any], bool]]]
        ] = None,
        path: Optional[Path] = None,
        submitter: Optional[AbstractSubmitter] = None,
    ) -> None:
        """
        Add a node with an optionally attached callable to the graph.

        Parameters
        ----------
        name : str
            Name of the node
        function_dict : None or dict
            Dict with the function to call
            On the form
            >>> {'function': None or callable,
            ...  'args': None or tuple,
            ...  'kwargs': None or dict}
        path : None or Path
            Absolute path to store the python file which holds the function and
            its arguments
        submitter : AbstractSubmitter
            Submitter to submit the function with
            If None, the default LocalSubmitter will be used

        Raises
        ------
        ValueError
            If the node is already present in the graph
        """
        if name in self.__node_set:
            raise ValueError(f"'{name}' is already present in the graph")

        if function_dict is None:
            function_dict = {"function": None, "args": None, "kwargs": None}

        submitter = submitter if submitter is not None else LocalSubmitter()

        logging.debug(
            "Adding node=%s with function_dict=%s, path=%s and submitter=%s",
            name,
            function_dict,
            path,
            submitter,
        )
        self.__graph.add_node(
            name,
            function=function_dict["function"],
            args=function_dict["args"],
            kwargs=function_dict["kwargs"],
            path=path,
            submitter=submitter,
            status="ready",
        )
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
        logging.debug("Adding edge from %s to %s", start_node, end_node)
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

    def get_waiting_for_tuple(self, start_node_name) -> Tuple[str, ...]:
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

    def change_status_node_and_dependencies(
        self, start_node_name, status: str = "errored"
    ) -> None:
        """
        Remove node and all nodes waiting for the specified node.

        Parameters
        ----------
        start_node_name : str
            Name of the node to remove
            All nodes waiting for start_node_name will be removed
        status : str
            Status to set on start_node_name and all its dependencies
        """
        nodes_to_remove = self.get_waiting_for_tuple(start_node_name)
        for node_name in nodes_to_remove:
            self.__graph.nodes[node_name]["status"] = status
            logging.debug("Changed status of %s to %s", node_name, status)

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
