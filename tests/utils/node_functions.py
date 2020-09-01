"""
Functions to test a complex network.

To visualize the network, run the following code, and paste the content to
http://www.webgraphviz.com

>>> import networkx as nx
>>> g = nx.DiGraph()
...
>>> for i in range(10):
>>>     g.add_node(i, status="ready")
...
>>> # The following nodes are meant as BOUT++ runs:
>>> # 2, 3, 4, 6, 9
>>> # Pre processors to 2
>>> g.add_edge(0, 2)
>>> g.add_edge(1, 2)
...
>>> # Post processors from 2
>>> g.add_edge(2, 3)
>>> g.add_edge(2, 5)
...
>>> g.add_edge(2, 6)
>>> g.add_edge(2, 7)
...
>>> # Pre processors to 9
>>> g.add_edge(4, 9)
>>> g.add_edge(6, 9)
>>> g.add_edge(7, 9)
...
>>> # Post processors to 9
>>> g.add_edge(9, 10)
...
>>> # Pre processor to 8
>>> g.add_edge(4, 8)
>>> g.add_edge(6, 8)
...
>>> # Post processor to 8
>>> g.add_edge(8, 10)
...
>>> print(nx.nx_pydot.to_pydot(g))
"""

from pathlib import Path


def node_zero(bout_run_directory_node_2: Path, pre_and_post_directory: Path) -> None:
    """
    Preprocess before node 2.

    FIXME: YOU ARE HERE: ALSO NEED TO TEST THAT NODE 2 IS ACTUALLY WAITING FOR 0 AND 1
           IN OTHER WORDS: THAT A BOUT RUN IS ACTUALLY WAITING FOR PRE PROCESSORS
           MAYBE NEED TO MAKE A SIMPLE CXX

    Parameters
    ----------
    bout_run_directory_node_2 : Path
        Directory where the dump files of the BOUT++ run of node 2 is stored
    pre_and_post_directory : Path
        Path where temporary files are stored to sign that nodes has been
        successfully processed
    """
    # Node 2 completed before node 0
    assert bout_run_directory_node_2.joinpath("BOUT.settings").is_file()
    with pre_and_post_directory.joinpath("1.txt").open("w") as file:
        file.write("Complete")


def node_one(bout_run_directory_node_2: Path, pre_and_post_directory: Path) -> None:
    """
    Preprocess before node 2.

    Parameters
    ----------
    bout_run_directory_node_2 : Path
        Directory where the dump files of the BOUT++ run of node 2 is stored
    pre_and_post_directory : Path
        Path where temporary files are stored to sign that nodes has been
        successfully processed

    """
    # Node 2 completed before node 1
    assert bout_run_directory_node_2.joinpath("BOUT.settings").is_file()
    with pre_and_post_directory.joinpath("1.txt").open("w") as file:
        file.write("Complete")


def node_five(bout_run_directory_node_2: Path, pre_and_post_directory: Path) -> None:
    """
    Postprocess after node 2.

    Parameters
    ----------
    bout_run_directory_node_2 : Path
        Directory where the dump files of the BOUT++ run of node 2 is stored
    pre_and_post_directory : Path
        Path where temporary files are stored to sign that nodes has been
        successfully processed
    """
    # Node 2 not completed before node 5
    assert not bout_run_directory_node_2.joinpath("BOUT.settings").is_file()
    with pre_and_post_directory.joinpath("5.txt").open("w") as file:
        file.write("Complete")


def node_seven(
    bout_run_directory_node_2: Path,
    bout_run_directory_node_9: Path,
    pre_and_post_directory: Path,
) -> None:
    """
    Postprocess after node 2, preprocess before node 9.

    Parameters
    ----------
    bout_run_directory_node_2 : Path
        Directory where the dump files of the BOUT++ run of node 2 is stored
    bout_run_directory_node_9 : Path
        Directory where the dump files of the BOUT++ run of node 9 is stored
    pre_and_post_directory : Path
        Path where temporary files are stored to sign that nodes has been
        successfully processed
    """
    # Node 9 completed before node 7
    assert bout_run_directory_node_9.joinpath("BOUT.settings").is_file()

    # Node 2 not completed before node 7
    assert not bout_run_directory_node_2.joinpath("BOUT.settings").is_file()

    with pre_and_post_directory.joinpath("7.txt").open("w") as file:
        file.write("Complete")


def node_eight(
    bout_run_directory_node_4: Path,
    bout_run_directory_node_6: Path,
    pre_and_post_directory: Path,
) -> None:
    """
    Postprocess after node 6 and 4, preprocess before node 10.

    Parameters
    ----------
    bout_run_directory_node_4 : Path
        Directory where the dump files of the BOUT++ run of node 4 is stored
    bout_run_directory_node_6 : Path
        Directory where the dump files of the BOUT++ run of node 6 is stored
    pre_and_post_directory : Path
        Path where temporary files are stored to sign that nodes has been
        successfully processed
    """
    # Node 4 not completed before node 8
    assert not bout_run_directory_node_4.joinpath("BOUT.settings").is_file()

    # Node 6 not completed before node 8
    assert not bout_run_directory_node_6.joinpath("BOUT.settings").is_file()

    # Node 8 completed before node 10
    assert pre_and_post_directory.joinpath("10.txt").is_file()

    with pre_and_post_directory.joinpath("8.txt").open("w") as file:
        file.write("Complete")


def node_ten(bout_run_directory_node_9: Path, pre_and_post_directory: Path) -> None:
    """
    Postprocess after node 9 and 8.

    Parameters
    ----------
    bout_run_directory_node_9 : Path
        Directory where the dump files of the BOUT++ run of node 6 is stored
    pre_and_post_directory : Path
        Path where temporary files are stored to sign that nodes has been
        successfully processed
    """
    # Node 9 completed before node 8
    assert not bout_run_directory_node_9.joinpath("BOUT.settings").is_file()

    # Node 10 started before completion of node 10
    assert not pre_and_post_directory.joinpath("8.txt").is_file()

    with pre_and_post_directory.joinpath("10.txt").open("w") as file:
        file.write("Complete")
