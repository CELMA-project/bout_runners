"""Contains unittests for the run group."""

from typing import Callable
from bout_runners.runner.run_graph import RunGraph
from bout_runners.runner.run_group import RunGroup
from bout_runners.runner.bout_run_setup import BoutRunSetup


def test_constructor(get_bout_run_setup: Callable[[str], BoutRunSetup]) -> None:
    """
    Test the constructor.

    Parameters
    ----------
    get_bout_run_setup : function
        Function which returns the BoutRunSetup object based on the conduction directory
    """
    bout_run_setup = get_bout_run_setup("test_run_group_constructor")
    run_graph = RunGraph()
    run_graph.add_function_node("1")

    run_group_0 = RunGroup(run_graph, bout_run_setup)
    assert run_group_0.bout_run_node_name == "bout_run_0"

    run_group_test = RunGroup(run_graph, bout_run_setup, name="test")
    assert run_group_test.bout_run_node_name == "bout_run_test"

    run_group_1 = RunGroup(run_graph, bout_run_setup, waiting_for="1")
    assert run_group_1.bout_run_node_name == "bout_run_1"

    expected = (
        "1",
        "bout_run_1",
    )
    assert expected == run_graph.get_waiting_for_tuple("1")


def test_pre_processor(get_bout_run_setup: Callable[[str], BoutRunSetup]) -> None:
    """
    Test the pre processor.

    Parameters
    ----------
    get_bout_run_setup : function
        Function which returns the BoutRunSetup object based on the conduction directory
    """
    bout_run_setup = get_bout_run_setup("test_run_group_pre")
    run_graph = RunGraph()
    run_graph.add_function_node("1")
    run_group_pre = RunGroup(
        run_graph, bout_run_setup, name="test_pre", waiting_for="1"
    )

    run_group_pre.add_pre_processor(lambda: None)
    run_group_pre.add_pre_processor(lambda: None)

    root_nodes = run_graph.pick_root_nodes()
    assert len(root_nodes) == 3


def test_post_processor(get_bout_run_setup: Callable[[str], BoutRunSetup]) -> None:
    """
    Test the post processor.

    Parameters
    ----------
    get_bout_run_setup : function
        Function which returns the BoutRunSetup object based on the conduction directory
    """
    bout_run_setup = get_bout_run_setup("test_run_group_post")
    run_graph = RunGraph()
    run_graph.add_function_node("1")
    run_group_post = RunGroup(
        run_graph, bout_run_setup, name="test_post", waiting_for="1"
    )

    run_group_post.add_post_processor(lambda: None)
    run_group_post.add_post_processor(lambda: None)

    expected = (
        "1",
        "bout_run_test_post",
        "post_processor_test_post_0",
        "post_processor_test_post_1",
    )
    assert set(expected) == set(run_graph.get_waiting_for_tuple("1"))
