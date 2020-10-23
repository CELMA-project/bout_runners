"""Contains the RunGroup class."""

import re
import logging
from pathlib import Path
from typing import Optional, Union, Iterable, List, Callable, Tuple, Any, Dict

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.runner.bout_run_setup import BoutRunSetup
from bout_runners.executor.bout_paths import BoutPaths
from bout_runners.runner.run_graph import RunGraph
from bout_runners.submitter.abstract_submitters import AbstractSubmitter
from bout_runners.submitter.submitter_factory import get_submitter


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
    __names : list of str
        List of the run group names, makes sure there will be no name collision
    __dst_dir : Path
        The path to the dump directory
    __name : str
        Name of the RunGroup
    __run_graph : RunGraph
        The getter variable of run_graph
    __bout_run_setup : BoutRunSetup
        The setup of the BOUT++ run
    __bout_run_node_name : str
        Getter variable for bout_run_node_name
    __pre_processors : list of str
        Getter variable for pre_processors
    __post_processors : list of str
        Getter variable for post_processors
    bout_run_node_name : str
        Name of the BOUT++ run part of the run group
    bout_paths : BoutPaths
        The BoutPaths of the BOUT++ run
    db_connector : DatabaseConnector
        The DatabaseConnector of the BOUT++ run
    run_graph : RunGraph
        The RunGraph which the RunGroup will be attached to
    pre_processors : list of str
        List of pre-processors (which will run before the BOUT++ run)
    post_processors : list of str
        List of post-processors (which will run after the BOUT++ run)

    Methods
    -------
    __increment_name()
        Increment the name with a number to avoid name collision
    add_pre_processor(function_dict, directory, submitter, waiting_for)
        Add a function which will run prior to the BOUT++ run
    add_post_processor(function_dict, directory, submitter, waiting_for)
        Add a function which will run after the BOUT++ run

    Examples
    --------
    The RunGroup contains the a bout run and it's pre- and post-processors

    >>> bout_run_setup = BoutRunSetup(executor, db_connector, final_parameters)
    >>> run_graph = RunGraph()
    >>> # Attach a RunGroups to the run_graph
    >>> run_group_1 = RunGroup(run_graph, bout_run_setup_1)
    >>> run_group_2 = RunGroup(run_graph, bout_run_setup_2)
    >>> # Add the function `foo` as a post-processor to run_group_1
    >>> post_processor_node_name = run_group_1.add_post_processor(
    ... {'function': foo, 'args': (foo_1,), 'kwargs':None})
    >>> # Add the function `bar` as a pre-processor to run_group_2,
    ... # which waits for the post-processor of run_group_1
    >>> run_group_2.add_pre_processor(
    ... {'function': bar, 'args': None, 'kwargs':None}),
    ... waiting_for=post_processor_node_name)
    >>> runner = BoutRunner(run_graph)
    >>> runner.run()

    See Also
    --------
    RunGraph : Class to create a run graph
    """

    __counter = 0
    __names: List[str] = list()

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
        RunGraph.add_function_node

        Parameters
        ----------
        run_graph : RunGraph
            The RunGraph which the RunGroup will be attached to
        bout_run_setup : BoutRunSetup
            The setup of the BOUT++ run
        name : None or str
            Name of the RunGroup
            If None, the class counter will be used
        waiting_for : None or str or iterable
            Name of nodes the name_of_waiting_node will wait for
        """
        self.__run_graph = run_graph
        self.__name = name
        self.__bout_run_setup = bout_run_setup
        self.__dst_dir = self.__bout_run_setup.bout_paths.bout_inp_dst_dir
        self.__pre_processors: List[str] = list()
        self.__post_processors: List[str] = list()

        if self.__name is None:
            self.__name = str(RunGroup.__counter)
            RunGroup.__counter += 1
        if self.__name in RunGroup.__names:
            self.__increment_name()
        RunGroup.__names.append(self.__name)

        # Assign a node to bout_run_setup
        self.__bout_run_node_name = f"bout_run_{self.__name}"
        self.__run_graph.add_bout_run_node(
            self.bout_run_node_name, self.__bout_run_setup
        )

        # Add edges to the nodes
        self.__run_graph.add_waiting_for(self.bout_run_node_name, waiting_for)

    def __increment_name(self) -> None:
        """Increment the name of the RunGroup."""
        old_name = self.__name if self.__name is not None else ""
        pattern = old_name + r"_(\d)+$"
        numbers = list()
        for name in RunGroup.__names:
            match = re.search(pattern, name)
            if match is not None:
                # NOTE: THe zeroth group is the matching string
                numbers.append(int(match.group(1)))
        if len(numbers) == 0:
            self.__name = f"{old_name}_1"
        else:
            self.__name = f"{old_name}_{max(numbers) + 1}"
        logging.warning(
            "%s is already registered as a RunGroup name. Changing the name to %s",
            old_name,
            self.__name,
        )

    @property
    def bout_run_node_name(self) -> str:
        """
        Return the name of the BOUT++ run node.

        Returns
        -------
        str
            The name of the BOUT++ run node
        """
        return self.__bout_run_node_name

    @property
    def run_graph(self) -> RunGraph:
        """
        Return the run graph.

        Returns
        -------
        RunGraph
            The run graph
        """
        return self.__run_graph

    @property
    def bout_paths(self) -> BoutPaths:
        """
        Return the BoutPaths.

        Returns
        -------
        BoutPaths
            The BoutPaths
        """
        return self.__bout_run_setup.bout_paths

    @property
    def db_connector(self) -> DatabaseConnector:
        """
        Return the DatabaseConnector.

        Returns
        -------
        BoutPaths
            The DatabaseConnector
        """
        return self.__bout_run_setup.db_connector

    @property
    def pre_processors(self) -> Tuple[str, ...]:
        """
        Return the pre_processors.

        Returns
        -------
        tuple
            The tuple of pre_processors
        """
        return tuple(self.__pre_processors)

    @property
    def post_processors(self) -> Tuple[str, ...]:
        """
        Return the post_processors.

        Returns
        -------
        tuple
            The tuple of post_processors
        """
        return tuple(self.__post_processors)

    def add_pre_processor(
        self,
        function_dict: Dict[
            str, Optional[Union[Callable, Tuple[Any, ...], Dict[str, Any]]]
        ],
        directory: Optional[Path] = None,
        submitter: Optional[AbstractSubmitter] = None,
        waiting_for: Optional[Union[str, Iterable[str]]] = None,
    ) -> str:
        """
        Add a pre-processor to the BOUT++ run.

        The function and the parameters will be saved to a python script which will
        be submitted

        Parameters
        ----------
        function_dict : dict
            Dict with the function to call
            On the form
            >>> {'function': callable,
            ...  'args': None or tuple,
            ...  'kwargs': None or dict}
        directory : None or Path
            Absolute path to directory to store the python script
            If None, the destination directory of BoutRun will be used
        submitter : AbstractSubmitter
            Submitter to submit the function with
            If None, the default LocalSubmitter will be used
        waiting_for : None or str or iterable
            Name of nodes this node will wait for to finish before executing

        Returns
        -------
        pre_processor_node_name : str
            The node name of the pre-processor

        Raises
        ------
        ValueError
            If the function in the function_dict is not callable
        """
        if directory is None:
            directory = self.__dst_dir

        if "function" not in function_dict.keys() or not callable(
            function_dict["function"]
        ):
            msg = 'function_dict["function"] must be callable'
            logging.error(msg)
            raise ValueError(msg)

        pre_processor_node_name = (
            f"pre_processor_{self.__name}_{len(self.__pre_processors)}"
        )
        path = directory.joinpath(
            f"{function_dict['function'].__name__}_{pre_processor_node_name}.py"
        )
        submitter = submitter if submitter is not None else get_submitter()
        self.__run_graph.add_function_node(
            pre_processor_node_name,
            function_dict=function_dict,
            path=path,
            submitter=submitter,
        )
        self.__run_graph.add_edge(pre_processor_node_name, self.bout_run_node_name)
        self.__run_graph.add_waiting_for(pre_processor_node_name, waiting_for)
        self.__pre_processors.append(pre_processor_node_name)
        return pre_processor_node_name

    def add_post_processor(
        self,
        function_dict: Dict[
            str, Optional[Union[Callable, Tuple[Any, ...], Dict[str, Any]]]
        ],
        directory: Optional[Path] = None,
        submitter: Optional[AbstractSubmitter] = None,
        waiting_for: Optional[Union[str, Iterable[str]]] = None,
    ) -> str:
        """
        Add a post-processor to the BOUT++ run.

        The function and the parameters will be saved to a python script which will
        be submitted

        Parameters
        ----------
        function_dict : dict
            Dict with the function to call
            On the form
            >>> {'function': callable,
            ...  'args': None or tuple,
            ...  'kwargs': None or dict}
        directory : None or Path
            Absolute path to directory to store the python script
            If None, the destination directory of BoutRun will be used
        waiting_for : None or str or iterable
            Name of nodes this node will wait for to finish before executing
        submitter : None or AbstractSubmitter
            Submitter to submit the function with
            If None, the default LocalSubmitter will be used

        Returns
        -------
        post_processor_node_name : str
            The node name of the pre-processor

        Raises
        ------
        ValueError
            If the function in the function_dict is not callable
        """
        if directory is None:
            directory = self.__dst_dir

        if "function" not in function_dict.keys() or not callable(
            function_dict["function"]
        ):
            msg = 'function_dict["function"] must be callable'
            logging.error(msg)
            raise ValueError(msg)

        post_processor_node_name = (
            f"post_processor_{self.__name}_{len(self.__post_processors)}"
        )
        path = directory.joinpath(
            f"{function_dict['function'].__name__}_{post_processor_node_name}.py"
        )
        submitter = submitter if submitter is not None else get_submitter()
        self.__run_graph.add_function_node(
            post_processor_node_name,
            function_dict=function_dict,
            path=path,
            submitter=submitter,
        )
        self.__run_graph.add_edge(self.bout_run_node_name, post_processor_node_name)
        self.__run_graph.add_waiting_for(post_processor_node_name, waiting_for)
        self.__post_processors.append(post_processor_node_name)
        return post_processor_node_name
