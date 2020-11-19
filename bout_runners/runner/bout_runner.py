"""Contains the BOUT runner class."""


import logging
from pathlib import Path
from time import sleep
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.metadata.status_checker import StatusChecker
from bout_runners.parameters.bout_run_setup import BoutRunSetup
from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.run_group import RunGroup
from bout_runners.submitter.abstract_cluster_submitter import AbstractClusterSubmitter
from bout_runners.submitter.abstract_submitter import AbstractSubmitter
from bout_runners.submitter.local_submitter import LocalSubmitter
from bout_runners.submitter.submitter_factory import get_submitter
from bout_runners.utils.file_operations import copy_restart_files


class BoutRunner:
    r"""
    Class for executing a run and store its metadata.

    Attributes
    ----------
    __run_graph : RunGraph
        Getter variable for executor the run graph
    run_graph : Graph
        The run graph to be executed
    wait_time : int
        Time to wait before checking if a job has completed

    Methods
    -------
    __add_waiting_for(node_name)
        Add the job_ids to wait for in the submission script
    __prepare_run(force, restart_all)
        Prepare the run sequence
    __updates_when_restart_all_is_true()
        Update paths and nodes when restart_all is True
    __inject_copy_restart_files_node(node_with_restart)
        Inject a node which copy restart files
    __make_restart_files_node(to_node_name, copy_restart_from, copy_restart_to)
        Make nodes which copies restart files
    __next_order_has_local(submitter_dict)
        Check if the current order of nodes has any local submitters
    __monitor_runs(submitter_dict, raise_errors)
        Monitor the runs belonging to the same order
    __run_status_checker(node_name)
        Run the StatusChecker
    __this_order_has_local(submitter_dict)
        Check if the current order of nodes has any local submitters
    __update_submitter_dict_after_run_bout_run(node_name, submitted, submitter_dict)
        Update the submitter dict after calling run_bout_run
    find_matching_order_number(node_names, node_orders)
        Return the order matching the node names
    run_bout_run(bout_run_setup, restart_from_bout_inp_dst, force)
        Perform the BOUT++ run and capture the related metadata
    run_function(path, function, args, kwargs, submitter)
        Submit a function for execution
    reset()
        Reset the run_graph
    release_nodes(nodes_to_release)
        Release nodes to a submission queue if applicable
    cluster_node_exist(node_names)
        Check if any of the nodes have a submitter of type AbstractClusterSubmitter
    wait_until_completed(self)
        Wait until all submitted nodes are completed
    run(restart_all, force, raise_errors)
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
    >>> from bout_runners.executor.executor import BoutRunExecutor
    >>> from bout_runners.database.database_connector import DatabaseConnector
    >>> from bout_runners.parameters.default_parameters import DefaultParameters
    >>> from bout_runners.parameters.run_parameters import RunParameters
    >>> from bout_runners.parameters.final_parameters import FinalParameters
    >>> from bout_runners.submitter.local_submitter import LocalSubmitter
    >>> from bout_runners.runner.bout_run_setup import BoutRunSetup
    >>> from bout_runners.runner.run_graph import RunGraph
    >>> from bout_runners.runner.run_group import RunGroup

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
    >>> executor = BoutRunExecutor(
    ...     bout_paths=bout_paths,
    ...     submitter=LocalSubmitter(bout_paths.project_path),
    ...     run_parameters=run_parameters)
    >>> db_connector = DatabaseConnector('name_of_database', db_root_path=Path())
    >>> bout_run_setup = BoutRunSetup(executor, db_connector, final_parameters)
    >>> run_graph = RunGraph()
    >>> # The RunGroup can attach pre and post-processors to the run
    >>> # See the user manual for more info
    >>> _ = RunGroup(run_graph, bout_run_setup, name='my_test_run')

    Run the project

    >>> runner = BoutRunner(run_graph)
    >>> runner.run()
    """

    def __init__(
        self, run_graph: Optional[RunGraph] = None, wait_time: int = 5
    ) -> None:
        """
        Set the member data.

        Parameters
        ----------
        run_graph : None or RunGraph
            The run graph to be executed
            If None the run graph will be constructed and added parameters from the
            default BoutRunSetup
        wait_time : int
            Time to wait before checking if a job has completed
        """
        self.wait_time = wait_time
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

    def __add_waiting_for(self, node_name: str) -> None:
        """
        Add the job_ids to wait for in the submission script.

        Parameters
        ----------
        node_name : str
            Name of current node
        """
        predecessors = self.__run_graph.predecessors(node_name)
        waiting_for = (
            self.__run_graph[p_name]["submitter"].job_id
            for p_name in predecessors
            if isinstance(
                self.__run_graph[p_name]["submitter"],
                AbstractClusterSubmitter,
            )
            and not self.__run_graph[p_name]["submitter"].completed()
        )
        self.__run_graph[node_name]["submitter"].add_waiting_for(waiting_for)

    def __prepare_run(self, force: bool, restart_all: bool) -> None:
        """
        Prepare the run sequence.

        If any bout_run nodes contain restart_from this function will create a
        node which copies the restart files

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
        """
        logging.info("Start: Preparing all runs")

        if force or restart_all:
            if restart_all:
                self.__updates_when_restart_all_is_true()
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
            logging.critical(msg)
            raise RuntimeError(msg)

        for node in tuple(self.__run_graph.nodes):
            if (
                node.startswith("bout_run")
                and self.__run_graph[node]["bout_run_setup"].executor.restart_from
                is not None
            ):
                logging.info(
                    "Found restart_from in node %s, "
                    "will inject node which copies restart files",
                    node,
                )
                self.__inject_copy_restart_files_node(node)
        logging.info("Done: Preparing all runs")

    def __updates_when_restart_all_is_true(self) -> None:
        """Update paths and nodes when restart_all is True."""
        logging.info("Updating executor.restart_from as restart_all=True")
        for node in tuple(self.__run_graph.nodes):
            if node.startswith("bout_run"):
                # Input must now point at previous destination
                self.__run_graph[node][
                    "bout_run_setup"
                ].bout_paths.bout_inp_src_dir = self.__run_graph[node][
                    "bout_run_setup"
                ].bout_paths.bout_inp_dst_dir
                self.__run_graph[node][
                    "bout_run_setup"
                ].executor.restart_from = self.__run_graph[node][
                    "bout_run_setup"
                ].bout_paths.bout_inp_src_dir
                # Any copy restart nodes must be copied
                for predecessor in self.__run_graph.predecessors(node):
                    if predecessor.startswith("copy_restart_files"):
                        if (
                            self.run_graph[predecessor]["function"].__module__
                            == "bout_runners.utils.file_operations"
                            and self.run_graph[predecessor]["function"].__name__
                            == "copy_restart_files"
                        ):
                            logging.info(
                                "Updating the arguments to %s as restart_all=True",
                                predecessor,
                            )
                            old_args = self.run_graph[predecessor]["args"]
                            new_args = (
                                self.__run_graph[node][
                                    "bout_run_setup"
                                ].executor.restart_from,
                                self.__run_graph[node][
                                    "bout_run_setup"
                                ].bout_paths.bout_inp_dst_dir,
                            )
                            logging.debug(
                                "Changing argument 'copy_restart_from' from %s to %s",
                                old_args[0],
                                new_args[0],
                            )
                            logging.debug(
                                "Changing argument 'copy_restart_to' from %s to %s",
                                old_args[1],
                                new_args[1],
                            )
                            self.run_graph[predecessor]["args"] = new_args
                        else:
                            logging.warning(
                                "restart_all=True, but node %s waits for %s which is "
                                "a function from %s. No updates to this node will be "
                                "made",
                                node,
                                predecessor,
                                self.run_graph[predecessor]["function"].__module__
                                + "."
                                + self.run_graph[predecessor]["function"].__name__,
                            )
                        break

    def __inject_copy_restart_files_node(self, node_with_restart: str) -> None:
        """
        Inject a node which copy restart files.

        Parameters
        ----------
        node_with_restart : str
            Name of the node which will wait for a restart node
        """
        logging.info("Start: Injecting node which copies restart files")
        restart_from = self.__run_graph[node_with_restart][
            "bout_run_setup"
        ].executor.restart_from
        copy_to = self.__run_graph[node_with_restart][
            "bout_run_setup"
        ].bout_paths.bout_inp_dst_dir

        copy_node = self.__make_restart_files_node(
            node_with_restart, restart_from, copy_to
        )

        for predecessor in self.__run_graph.predecessors(node_with_restart):
            self.__run_graph.remove_edge(predecessor, node_with_restart)
            self.__run_graph.add_edge(predecessor, copy_node)

        self.__run_graph.add_edge(copy_node, node_with_restart)
        logging.info("Done: Injecting node which copies restart files")

    def __make_restart_files_node(
        self, to_node_name: str, copy_restart_from: Path, copy_restart_to: Path
    ) -> str:
        """
        Make nodes which copies restart files.

        Parameters
        ----------
        to_node_name : str
            Name of the node which will wait for a restart node
        copy_restart_from : Path
            Path to copy restart files from
        copy_restart_to : Path
            Path to copy restart files to

        Returns
        -------
        current_node_name : str
            Name of the node which copies files
        """
        current_node_name = (
            f"copy_restart_files_from_{copy_restart_from.name}_to_"
            f"{copy_restart_to.name}_for_{to_node_name}"
        )
        function_dict: Dict[
            str, Optional[Union[Callable, Tuple[Any, ...], Dict[str, Any]]]
        ] = {
            "function": copy_restart_files,
            "args": (copy_restart_from, copy_restart_to),
            "kwargs": None,
        }

        path = copy_restart_to.joinpath(f"{current_node_name}.py")
        submitter = get_submitter()
        if isinstance(submitter, AbstractClusterSubmitter):
            submitter.store_dir = copy_restart_to
            submitter.job_name = current_node_name
        self.__run_graph.add_function_node(
            name=current_node_name,
            function_dict=function_dict,
            path=path,
            submitter=submitter,
        )
        return current_node_name

    def __next_order_has_local(
        self,
        submitter_dict: Dict[
            str,
            Dict[
                str, Union[Optional[AbstractSubmitter], Union[DatabaseConnector, Path]]
            ],
        ],
    ) -> bool:
        """
        Check if the current order of nodes has any local submitters.

        Parameters
        ----------
        submitter_dict : dict
            Dict containing the the node names as keys and a new dict as values
            The new dict contains the keywords 'submitter' with value AbstractSubmitter

        Returns
        -------
        bool
            True if the current order has local submitters
        """
        for node_name in submitter_dict.keys():
            for successor_name in self.__run_graph.successors(node_name):
                if isinstance(
                    self.__run_graph[successor_name]["submitter"], LocalSubmitter
                ):
                    logging.info(
                        "%s in the next node order is of local submitter type, "
                        "will monitor this node order",
                        successor_name,
                    )
                    return True
        return False

    def __monitor_runs(
        self,
        submitter_dict: Dict[
            str,
            Dict[
                str, Union[Optional[AbstractSubmitter], Union[DatabaseConnector, Path]]
            ],
        ],
        raise_errors: bool,
    ) -> None:
        """
        Monitor the runs belonging to the same order.

        Parameters
        ----------
        submitter_dict : dict
            Dict containing the the node names as keys and a new dict as values
            The new dict contains the keywords 'submitter' with value AbstractSubmitter
            If the submitter contains a bout run, the new dict will also contain the
            keyword 'db_connector' with the value DatabaseConnector and the keyword
            'project_path' with the value Path which will be used in the StatusChecker
        raise_errors : bool
            If True the program will raise any error caught when during the running
            of the nodes
            If False the program will continue execution, but all nodes depending on
            the errored node will be marked as errored and not submitted

        Raises
        ------
        RuntimeError
            If the types in the dict are unexpected
        """
        logging.info("Start: Monitoring jobs at current order")
        node_names = list(node_name for node_name in submitter_dict.keys())
        while len(node_names) != 0:
            for node_name in node_names:
                submitter = submitter_dict[node_name]["submitter"]
                if not isinstance(submitter, AbstractSubmitter):
                    msg = (
                        f"The submitter of the '{node_name}' node was expected to be "
                        f"of type 'AbstractSubmitter', but got '{type(submitter)}' "
                        f"instead"
                    )
                    logging.critical(msg)
                    raise RuntimeError(msg)

                if submitter.completed():
                    if submitter.errored():
                        self.__run_graph.change_status_node_and_dependencies(node_name)
                        if raise_errors:
                            submitter.raise_error()

                    node_names.remove(node_name)
                else:
                    logging.debug(
                        "job_id=%s found, %s seems to be running",
                        submitter.job_id,
                        node_name,
                    )

                if node_name.startswith("bout_run"):
                    self.__run_status_checker(node_name)

            sleep(self.wait_time)
        logging.info("Done: Monitoring jobs at current order")

    def __run_status_checker(self, node_name: str) -> None:
        """
        Run the StatusChecker.

        Parameters
        ----------
        node_name : str
            Name of node to run the status checker for

        Raises
        ------
        RuntimeError
            If the types of self.__run_graph[node_name]["db_connector"] or
            self.__run_graph[node_name]["project_path"] are unexpected
        """
        db_connector = self.__run_graph[node_name]["db_connector"]
        if not isinstance(db_connector, DatabaseConnector):
            raise RuntimeError(
                f"The db_connector of the '{node_name}' node was expected "
                f"to be of type 'DatabaseConnector', but got "
                f"'{type(db_connector)}' instead"
            )
        project_path = self.__run_graph[node_name]["project_path"]
        if not isinstance(project_path, Path):
            raise RuntimeError(
                f"The project_path of the '{node_name}' node was expected "
                f"to be of type 'Path', but got '{type(project_path)}' "
                f"instead"
            )
        StatusChecker(db_connector, project_path).check_and_update_status()

    @staticmethod
    def __this_order_has_local(
        submitter_dict: Dict[
            str,
            Dict[
                str, Union[Optional[AbstractSubmitter], Union[DatabaseConnector, Path]]
            ],
        ]
    ) -> bool:
        """
        Check if the current order of nodes has any local submitters.

        Parameters
        ----------
        submitter_dict : dict
            Dict containing the the node names as keys and a new dict as values
            The new dict contains the keywords 'submitter' with value AbstractSubmitter

        Returns
        -------
        bool
            True if the current order has local submitters
        """
        for node_name in submitter_dict.keys():
            if isinstance(submitter_dict[node_name]["submitter"], LocalSubmitter):
                logging.debug(
                    "%s is of local submitter type, will monitor this node order",
                    node_name,
                )
                return True
        return False

    def __update_submitter_dict_after_run_bout_run(
        self,
        node_name: str,
        submitted: bool,
        submitter_dict: Dict[
            str,
            Dict[
                str,
                Union[Optional[AbstractSubmitter], Union[DatabaseConnector, Path]],
            ],
        ],
    ) -> None:
        """
        Update the submitter dict after calling run_bout_run.

        If the run has been submitted we add information about the database in the dict.
        Else we pop the node name from the dict in order not to monitor it.

        Parameters
        ----------
        node_name : str
            Name of current node
        submitted : bool
            Whether or not the run was submitted
        submitter_dict : dict
            Dict containing the the node names as keys and a new dict as values
            The new dict contains the keywords 'submitter' with value AbstractSubmitter
        """
        if submitted:
            self.__run_graph[node_name]["db_connector"] = self.__run_graph[node_name][
                "bout_run_setup"
            ].db_connector
            self.__run_graph[node_name]["project_path"] = self.__run_graph[node_name][
                "bout_run_setup"
            ].bout_paths.project_path
        else:
            submitter_dict.pop(node_name)

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
    def find_matching_order_number(
        node_names: Tuple[str, ...], node_orders: Tuple[Tuple[str, ...], ...]
    ) -> Optional[int]:
        """
        Return the order matching the node names.

        Parameters
        ----------
        node_names : tuple of str
            Node names
        node_orders : tuple of tuple of str
            Ordered tuple of orders

        Returns
        -------
        order_number : int or None
            The first order where a match was found
            If no match was found 0 is returned
        """
        order_number = -1
        found = False
        for order_nodes in node_orders:
            for node_name in node_names:
                if node_name in order_nodes:
                    found = True
                    break
                order_number += 1
        if found:
            return order_number
        return None

    @staticmethod
    def run_bout_run(
        bout_run_setup: BoutRunSetup,
        force: bool = False,
    ) -> bool:
        """
        Perform the BOUT++ run and capture the related metadata.

        Parameters
        ----------
        bout_run_setup : BoutRunSetup
            The setup for the BOUT++ run
        force : bool
            Execute the run even if has been performed with the same parameters

        Returns
        -------
        bool
            Whether or not the run was submitted
        """
        restart = bool(bout_run_setup.executor.restart_from)

        if restart and force:
            logging.warning(
                "force has been set to True for a run which is to use restart files. "
                "Will therefore ignore force"
            )

        run_id = bout_run_setup.metadata_recorder.capture_new_data_from_run(
            bout_run_setup.executor.submitter.processor_split, restart, force
        )

        if run_id is None:
            if not restart:
                logging.info("Executing the run")
            else:
                logging.info("Executing the run from restart files")
            bout_run_setup.executor.execute(restart)
        else:
            logging.warning(
                "Run with the same configuration has been executed before, "
                "see run with run_id %d",
                run_id,
            )
            if force:
                logging.info("Executing the run as force==True")
                bout_run_setup.executor.execute()
            else:
                return False

        return True

    @staticmethod
    def run_function(
        path: Path,
        submitter: AbstractSubmitter,
        function: Callable,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> AbstractSubmitter:
        """
        Submit a function for execution.

        Parameters
        ----------
        path : Path
            Absolute path to store the python file which holds the function and
            its arguments
        submitter : AbstractSubmitter
            The submitter to submit the function with
            Uses the default LocalSubmitter if None
        function : function
            The function to call
        args : None or tuple
            The positional arguments
        kwargs : None or dict
            The keyword arguments

        Returns
        -------
        submitter : AbstractSubmitter
            The submitter used
        """
        logging.info(
            "Submitting %s, with positional parameters %s, and keyword parameters %s",
            function.__name__,
            args,
            kwargs,
        )
        submitter.write_python_script(path, function, args, kwargs)
        command = f"python3 {path}"
        submitter.submit_command(command)
        return submitter

    def reset(self) -> None:
        """Reset the run_graph."""
        logging.debug("Resetting the graph")
        self.__run_graph.reset()

    def release_nodes(self, nodes_to_release: Tuple[Tuple[str, ...], ...]) -> None:
        """
        Release nodes to a submission queue if applicable.

        Parameters
        ----------
        nodes_to_release : iterable
            Name of nodes to release
        """
        if len(nodes_to_release) != 0:
            logging.info("Start: Releasing held cluster nodes")
            logging.debug("Release order: %s", nodes_to_release)
            for order in nodes_to_release:
                for node in order:
                    if isinstance(
                        self.__run_graph[node]["submitter"], AbstractClusterSubmitter
                    ):
                        self.__run_graph[node]["submitter"].release()
            logging.info("Done: Releasing held cluster nodes")

    def cluster_node_exist(self, node_names: Iterable[str]) -> bool:
        """
        Check if any of the nodes have a submitter of type AbstractClusterSubmitter.

        Parameters
        ----------
        node_names : iterable of str
            Iterable containing node names

        Returns
        -------
        bool
            Whether the iterable contains any cluster nodes
        """
        for node in node_names:
            if isinstance(
                self.__run_graph[node]["submitter"], AbstractClusterSubmitter
            ):
                return True
        return False

    def wait_until_completed(self) -> None:
        """Wait until all submitted nodes are completed."""
        logging.info("Start: Waiting for all submitted jobs to complete")
        for node_name in self.__run_graph.nodes:
            if self.__run_graph[node_name]["status"] == "submitted":
                self.__run_graph[node_name]["submitter"].wait_until_completed()
                self.__run_graph[node_name]["status"] = "completed"
                if node_name.startswith("bout_run"):
                    self.__run_status_checker(node_name)
        logging.info("Done: Waiting for all submitted jobs to complete")

    def run(
        self, restart_all: bool = False, force: bool = False, raise_errors: bool = True
    ) -> None:
        """
        Execute all the nodes in the run_graph.

        Parameters
        ----------
        restart_all : bool
            All the BOUT++ runs in the run graph will be restarted
        force : bool
            Execute the run even if has been performed with the same parameters
        raise_errors : bool
            If True the program will raise any error caught when during the running
            of the nodes
            If False the program will continue execution, but all nodes depending on
            the errored node will be marked as errored and not submitted
        """
        logging.info("Start: Calling .run() in BoutRunners")
        self.__prepare_run(force, restart_all)
        logging.debug("Dot-graph of the run\n%s", self.__run_graph.get_dot_string())

        for nodes_at_current_order in self.__run_graph:
            logging.info("Start: Processing nodes at current order")
            submitter_dict: Dict[
                str,
                Dict[
                    str,
                    Union[Optional[AbstractSubmitter], Union[DatabaseConnector, Path]],
                ],
            ] = dict()
            for node_name in nodes_at_current_order:
                if self.__run_graph[node_name]["status"] != "ready":
                    logging.info(
                        "Skipping node '%s' as it has status=%s",
                        node_name,
                        self.__run_graph[node_name]["status"],
                    )
                    continue
                logging.info("Start: Processing %s", node_name)
                if isinstance(
                    self.__run_graph[node_name]["submitter"],
                    AbstractClusterSubmitter,
                ):
                    self.__add_waiting_for(node_name)

                submitter_dict[node_name] = dict()
                submitter_dict[node_name]["submitter"] = self.__run_graph[node_name][
                    "submitter"
                ]
                if node_name.startswith("bout_run"):
                    submitted = self.run_bout_run(
                        self.__run_graph[node_name]["bout_run_setup"],
                        force,
                    )
                    self.__update_submitter_dict_after_run_bout_run(
                        node_name, submitted, submitter_dict
                    )
                else:
                    self.run_function(
                        self.__run_graph[node_name]["path"],
                        self.__run_graph[node_name]["submitter"],
                        self.__run_graph[node_name]["function"],
                        self.__run_graph[node_name]["args"],
                        self.__run_graph[node_name]["kwargs"],
                    )

                self.__run_graph[node_name]["status"] = "submitted"
                logging.info("Done: Processing %s", node_name)

            # We only monitor the runs if any local_submitters are present in
            # the current or the next order
            # Else the clusters will handle the monitoring
            monitor_run = False
            if self.__this_order_has_local(
                submitter_dict
            ) or self.__next_order_has_local(submitter_dict):
                monitor_run = True

            if monitor_run:
                if self.cluster_node_exist(self.__run_graph.nodes):
                    logging.warning(
                        "Mixed local and cluster nodes found in graph. "
                        "Releasing the cluster nodes up until the order of the "
                        "LocalSubmitter. This can cause a node waiting for one of "
                        "these nodes to be submitted after those nodes have finished "
                        "so that the cluster will reject those jobs."
                    )
                    reverse_sorted_node_orders = self.__run_graph.get_node_orders(
                        reverse=True
                    )
                    order_number = self.find_matching_order_number(
                        tuple(submitter_dict.keys()), reverse_sorted_node_orders
                    )
                    orders_to_release = reverse_sorted_node_orders[order_number:]
                    self.release_nodes(orders_to_release)
                    # We also need to release the current order in case
                    # the graph is not connected
                    self.release_nodes((tuple(submitter_dict.keys()),))
                self.__monitor_runs(submitter_dict, raise_errors)
            logging.info("Done: Processing nodes at current order")

        if self.cluster_node_exist(self.__run_graph.nodes):
            reverse_sorted_node_orders = self.__run_graph.get_node_orders(reverse=True)
            self.release_nodes(reverse_sorted_node_orders)
        logging.info("Done: Calling .run() in BoutRunners")
