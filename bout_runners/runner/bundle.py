"""Contains the bundle class."""


import logging
from typing import Optional, Callable, Tuple, Any, Dict, List
import networkx as nx

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_creator import DatabaseCreator
from bout_runners.executor.executor import Executor
from bout_runners.metadata.metadata_recorder import MetadataRecorder
from bout_runners.parameters.final_parameters import FinalParameters


class RunSetup:
    """
    Class for setting up the run.

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
    FIXME: Should have a RunGraph (contains the overall graph) where RunGroups
    (contains the subgraphs) can be added, thus Bundle may be obsolete.


    >>> run_setup = RunSetup(executor, db_connector, final_parameters)
    >>> run_group = RunGroup(run_setup)
    >>> run_bundle = RunBundle().add_run_group(run_group)
    >>> runner = BoutRunner(run_bundle)
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
    def __init__(self):
        self.graph = nx.DiGraph()
        self.nodes = set(self.graph.nodes)

    def add_node(self, name, function, args, kwargs):
        self.graph.add_node(name, function=function, args=args, kwargs=kwargs)
        self.nodes = set(self.graph.nodes)

    def add_edge(self, from_node, to_node):
        self.graph.add_edge(from_node, to_node)
        if not nx.is_directed_acyclic_graph(self.graph):
            raise ValueError(f"The node connection from {from_node} to {to_node} "
                             f"resulted in a cyclic graph")

    def pick_root(self):
        """Removes node from graph"""
        roots = tuple(node for node, degree in self.graph.in_degree() if degree == 0)
        root_nodes = list()
        for root in roots:
            root_nodes.append(self.graph[root])
            self.graph.remove_node(root)
        self.nodes = set(self.graph.nodes)
        return root_nodes

        # FIXME: Access a node attribute by self.graph.nodes[name][attribute]
        # FIXME: Can the BOUT++ run be formulated as a function? Setup?


class RunGroup:
    """
    Class for building a run group.

    A run group must contain one, and only one, recipe for executing the project (
    called `run_setup`). It may consist of one or several pre-hooks (functions that
    will run prior to the project execution) together with one or several post-hooks
    (functions that will run after the project execution).

    The pre-hooks may be chained (i.e. executed in a series) to each other,
    but at least one pre-hook is chained to the project execution (i.e. when that
    pre-hook is finished the execution of the project will start). Similarly the
    post-hooks may be chained to each other, but at least one is chained to the
    project execution (i.e. it will start when the project execution is done).
    """

    # NOTE: As the execution id must be unique over several instances of this class
    #       we make execution_id_counter a class bound variable
    execution_id_counter = 0

    def __init__(self, run_setup: RunSetup) -> None:
        """
        Initiate the member data and assign a run_setup_id.

        Parameters
        ----------
        run_setup : RunSetup
            The setup of the project execution
        """
        self.__run_id = None
        # FIXME: __run_waiting_for could be input parameter
        self.__run_waiting_for: Optional[int] = None
        self.__pre_hooks: List[Dict[str, Any]] = list()
        self.__post_hooks: List[Dict[str, Any]] = list()

        # FIXME: property for run_setup
        self.run_setup = run_setup
        self.__run_setup_id = RunGroup.execution_id_counter
        RunGroup.execution_id_counter += 1

        # FIXME: Redo some?
        self.__waiting_for_dict = dict()
        self.__execution_id_type = dict()

    @property
    def run_id(self) -> Optional[int]:
        """Return the id of the run."""
        return self.__run_id

    @property
    def run_waiting_for(self) -> Optional[int]:
        """
        Set the id which the run is waiting for.

        Returns
        -------
        self.__run_waiting_for : int
            The id which the run will wait for finishes before starting the run.
        """
        return self.__run_waiting_for

    @run_waiting_for.setter
    def run_waiting_for(self, waiting_for_id: int) -> None:
        self.__run_waiting_for = waiting_for_id

    def add_pre_hook(
        self,
        function: Callable,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        waiting_for_id: Optional[int] = None,
    ) -> int:
        """
        Add a pre hook function.

        The pre hooks will run prior to the project run.

        Parameters
        ----------
        function : callable
            The function to call
        args : None or tuple
            The positional arguments of the function
        kwargs : None or dict
            The keyword arguments of the function
        waiting_for_id : None or int
            The id of the function which have to finish before starting the function

        Returns
        -------
        hook_id : int
            The id of the hook
        """
        # Networkx has nice documentation
        # https://github.com/networkx/networkx/blob/master/doc/reference/index.rst
        # https://networkx.github.io/documentation/stable/reference/algorithms/approximation.html
        # https://github.com/networkx/networkx/blob/master/networkx/algorithms/approximation/__init__.py
        hook_id = RunGroup.execution_id_counter
        # Ensure that this is a true pre-hook
        # A pre-hook cannot wait for the current self._run_setup_id
        # A pre-hook cannot wait for a post-hook in the run_group
        # A pre-hook can wait for any other ids
        # A post-hook can wait for main run, or post hook in same group

        # FIXME: YOU ARE HERE
        # We know the current project run id, we know the previous project run id
        # If we also know the post-hook of the previous project run id, then we know
        # the full set of possible acceptable waiting_for_ids (this includes these
        # pre-hooks)
        # Will this limit alterations to the previous RunGroup? Maybe not
        # If each RunGroup can inherit from a parent which keeps track of all other
        # RunGroups
        # The parent has the overview: List of RunGroups?

        # FIXME: YOU ARE HERE - maybe the parent class can take care of the graph?
        #  Naming should be RunGroup-x-pre_hook_y etc,
        #  Can allow for a run to be waiting for two other runs (main run). One
        #  RunGroup holds a subgraph? One could pop from graph after a run is
        #  complete. Start if graph is always runned (if not waiting for)...use luigi
        #  or airflow?? Subgraphs:...makes no sense to keep it as separate
        #  subgraphs...next step: Play around with igraph
        # In[10]:
        # import networkx as nx
        # ...:
        # import matplotlib.pyplot as plt
        # ...:
        # ...: G = nx.DiGraph()
        # ...:
        # ...: map(G.add_node, range(12))
        # ...:
        # ...: G.add_edge(1, 0)
        # ...: G.add_edge(2, 0)
        # ...:
        # ...: G.add_edge(0, 3)
        # ...: G.add_edge(0, 4)
        # ...: G.add_edge(0, 6)
        # ...: G.add_edge(0, 7)
        # ...:
        # ...: G.add_edge(6, 5)
        # ...: G.add_edge(7, 5)
        # ...:
        # ...: G.add_edge(5, 8)
        # ...: print(nx.is_directed_acyclic_graph(G))
        # ...:
        # ...:
        # ...:  # Plot
        # ...: nx.nx_pydot.to_pydot(G).write_svg('delme.svg')
        # ...:
        # ...: G.add_edge(8, 2)
        # ...: print(nx.is_directed_acyclic_graph(G))
        # import networkx as nx
        # r1 = nx.DiGraph()
        # r1.add_node(0)
        # pre1 = nx.DiGraph()
        # pre1.add_nodes_from([1, 2])
        # def depends_on(to_g, from_g, to_node, from_nodes):
        #     to_g.add_nodes_from(from_g)
        #     for node in from_nodes:
        #         to_g.add_edge(node, to_node)
        #     if not nx.is_directed_acyclic_graph(to_g):
        #         raise ValueError("The resulting graph must be acyclic")
        #     return to_g
        # r1 = depends_on(r1, pre1, 0, [1,2])
        # nx.nx_pydot.to_pydot(r1).write_svg('delme2.svg')
        # r1 = depends_on(r1, pre1, 0, [1,2])
        # nx.nx_pydot.to_pydot(r1).write_svg('delme2.svg')
        # r1 = depends_on(r1, pre1, 0, [9,999])
        # nx.nx_pydot.to_pydot(r1).write_svg('delme2.svg')
        # pre1.add_nodes_from([1, 2, 777])
        # r1 = depends_on(r1, pre1, 0, [1, 2])
        # nx.nx_pydot.to_pydot(r1).write_svg('delme2.svg')
        if waiting_for_id in self.__execution_id_type.keys():
            pass

        RunGroup.execution_id_counter += 1
        pre_hook = {
            "function": function,
            "args": args,
            "kwargs": kwargs,
            "waiting_for_id": waiting_for_id,
            "hook_id": hook_id,
        }
        self.__pre_hooks.append(pre_hook)

        if hook_id in self.__waiting_for_dict.keys():
            self.__waiting_for_dict[hook_id] = [waiting_for_id]
        else:
            self.__waiting_for_dict[hook_id].append(waiting_for_id)

        return hook_id

    def add_post_hook(
        self,
        function: Callable,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        waiting_for_id: Optional[int] = None,
    ) -> int:
        """
        Run me.

        # FIXME: YOU ARE HERE
        # The question is now: How to make an execution order which waits for each other
        # In the docstring of Bundle.setup, there is a metacode which uses lists of list
        # IT LOOKS LIKE IT WILL WORK: IT UPDATES FINISHED RUNS TO NONE
        # ALWAYS RUN THE RUNS WHERE WAITING FOR IS NONE
        # THE QUESTION IS STILL: HOW TO MAKE SURE A PRE HOOK IS A PRE HOOK AND A POST
        # HOOK IS A POST
        # Maybe: Can annotate the waiting for dict with a dict which explains if it
        # is a run, pre-hook or post-hook, and which run-id it's tied to...but this
        # maybe prevents the user to add a run in-between two other runs


        # Maybe we are going into implementation territory: In PBS you can submit and
        # say waiting for

        g1 = {9:[6], 6:[7,8], 7:[1], 8:[None], 4:[1], 5:[1], 1:[2,3], 2:[None], 3:[None]}

        In [8]: from graphviz import Digraph
           ...:
           ...: g = Digraph('G', filename='hello.gv')
           ...:
           ...: for key in g1.keys():
           ...:     for el in g1[key]:
           ...:         if el is None:
           ...:             continue
           ...:         g.edge(f"{el}", f"{key}")
           ...:
           ...: g.view()

        # FIXME: How to enforce that this is an actual post hook?
          Could add type in dict over? But run id < pre hook id. Could say it has to be
          higher than pre hook id, and lower than highest post hook?
          The same problem with pre hooks...how can we ensure that it is earliest
          connected to the previous run_id?
        """
        hook_id = RunGroup.execution_id_counter
        # Ensure that this is a true pre-hook
        # FIXME: Do this

        RunGroup.execution_id_counter += 1
        post_hook = {
            "function": function,
            "args": args,
            "kwargs": kwargs,
            "waiting_for_id": waiting_for_id,
            "hook_id": hook_id,
        }
        self.__post_hooks.append(post_hook)

        if hook_id in self.__waiting_for_dict.keys():
            self.__waiting_for_dict[hook_id] = [waiting_for_id]
        else:
            self.__waiting_for_dict[hook_id].append(waiting_for_id)

        return hook_id


class Bundle:
    """Run a bundle."""

    def __init__(self,) -> None:
        """Construct lol."""

    def setup(self):
        """
        Call the pass function.

        # FIXME: Make documentation in readthedocs
        # FIXME: Make docstring documentation
        # FIXME: Make unittest
        # FIXME: Can loop through nested list...[[we,run,together],[wait_for]]?

        # Populate the waiting_for_dict
        waiting_for_dict = {None: list()}
        for group in run_group:
            # Add the pre_hooks
            for pre_hook in pre_hooks:
                if pre_hook.waiting_for_id in waiting_for_dict:
                    waiting_for_dict[pre_hook.waiting_for_id].append(pre_hook)
                else:
                    waiting_for_dict[pre_hook.waiting_for_id] = [pre_hook]

            # Add the run
            if pre_hook.waiting_for_id in waiting_for_dict:
                waiting_for_dict[run_group.run.waiting_for_id].append(run_group.run)
            else:
                waiting_for_dict[run_group.run.waiting_for_id] = [run_group.run]

            # Add the post_hooks
            for post_hook in post_hooks:
                if post_hook.waiting_for_id in waiting_for_dict:
                    waiting_for_dict[post_hook.waiting_for_id].append(post_hook)
                else:
                    waiting_for_dict[post_hook.waiting_for_id] = [post_hook]

        # FIXME: Enable parallel execution on single machine, user can specify max nodes
        # FIXME: PBS system may have this functionality built-in already (check old
        #        bout_runners)
        # FIXME: Add monitor: Execute the next in line when pid has finished. If not
        #        success -> broken chain, but the rest can continue
        # multiprocessing.Queue([maxsize])
        # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Queue
        # NOTE: An element may wait for more than one element
        # Upon submitting
        submission_pid = dict()
        ...
        # NOTE: object_id is the object_id of either pre_hook, run_group.run or
        post_hook

        # Remove element from queue on submission
        waiting_for_dict[None].remove(submission_id[completed_pid])



        submission_pid[object_id] = pid
        # FIXME: Find the inverse of this dict as well

        # Assume monitoring found that one of the runs were done
        # Put those marked as waiting for the completed pid in the None key of
        # waiting_for_dict
        waiting_for_dict[None].\
            append(waiting_for_dict.pop(submission_id[completed_pid]))
        # If an element waits for several ids
        for key in waiting_for_dict.keys():
            if type(key) == list:
                if submission_id[completed_pid] in key:
                    if len(key) == 1:
                        waiting_for_dict[None].\
                            append(waiting_for_dict.pop(submission_id[completed_pid]))
                    else:
                        new_key = key.copy()
                        new_key.remove(submission_id[completed_pid])
                        waiting_for_dict[new_key] = waiting_for_dict.pop(key)

        pre_hook
        run
        post_hook
        next keyword?

        two names: one for pre, run and post, another one for group of the preceeding
        run_group - can only contain one run (recipe?) but unlimited pre and post hooks
        run_bundle - a collection of run groups

        chain must have a start and an end...implemented as waits_for and tag_id
        cannot be two of the same ids in a bundle
        add_pre_hook - as many as you like, group them (list so you can add self.hooks)
        add_run - not possible to add_pre_hook (unless you specify the tag_id)

        must be possible to write "for group in bundle"
        must be possible to submit hooks with individual runners
        can also make hooks wait for each other
        possible to attach process ids to hooks

        must have a nice string representation

        How should BoutRunners be rewritten?
        For the most, that just builds up the run object
        Ideally we would have something that takes the bundle as input and loops through
        If no bundle is entered, will create a run_group with the run, and added to
        bundle
        !!! Possible to refactor BoutRunners to run_group
        ??? Is bundle just a list??? - if so the chaining could be weird,
        bundle should be able to sort? Unless the sort is meaningless

        Currently BoutRunners will only take a bundle and run, what else...
        prune hooks if run has already been run
        """

    def teardown(self):
        """Call something that does not exist."""
