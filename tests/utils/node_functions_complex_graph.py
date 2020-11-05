"""
Functions to test the complex network graph.

To visualize the network, run the following code, and paste the content to
http://www.webgraphviz.com

>>> import networkx as nx
>>> g = nx.DiGraph()
...
>>> for i in range(11):
>>>     g.add_node(i, status="ready")
...
>>> # The following nodes are meant as BOUT++ runs:
>>> # 2, 3, 4, 6, 9
>>> # Pre-processors to 2
>>> g.add_edge(0, 2)
>>> g.add_edge(1, 2)
...
>>> # Post-processors from 2
>>> g.add_edge(2, 3)
>>> g.add_edge(2, 5)
...
>>> g.add_edge(2, 6)
>>> g.add_edge(2, 7)
...
>>> # Pre-processors to 9
>>> g.add_edge(4, 9)
>>> g.add_edge(6, 9)
>>> g.add_edge(7, 9)
...
>>> # Post-processors to 9
>>> g.add_edge(9, 10)
...
>>> # Pre-processor to 8
>>> g.add_edge(4, 8)
>>> g.add_edge(6, 8)
...
>>> # Post-processor to 8
>>> g.add_edge(8, 10)
...
>>> print(nx.nx_pydot.to_pydot(g))
"""

from typing import Union
from pathlib import Path


def node_zero(
    bout_run_directory_node_2: Union[Path, str],
    pre_and_post_directory: Union[Path, str],
) -> None:
    """
    Preprocess before node 2.

    Parameters
    ----------
    bout_run_directory_node_2 : Path or str
        Directory where the dump files of the BOUT++ run of node 2 is stored
    pre_and_post_directory : Path or str
        Path where temporary files are stored to sign that nodes has been
        successfully processed
    """
    # Node 0 completed before node 2
    assert not Path(bout_run_directory_node_2).joinpath("BOUT.settings").is_file()
    with Path(pre_and_post_directory).joinpath("0.txt").open("w") as file:
        file.write("Complete")


def node_one(
    bout_run_directory_node_2: Union[Path, str],
    pre_and_post_directory: Union[Path, str],
) -> None:
    """
    Preprocess before node 2.

    Parameters
    ----------
    bout_run_directory_node_2 : Path or str
        Directory where the dump files of the BOUT++ run of node 2 is stored
    pre_and_post_directory : Path or str
        Path where temporary files are stored to sign that nodes has been
        successfully processed

    """
    # Node 1 completed before node 2
    assert not Path(bout_run_directory_node_2).joinpath("BOUT.settings").is_file()
    with Path(pre_and_post_directory).joinpath("1.txt").open("w") as file:
        file.write("Complete")


def node_five(
    bout_run_directory_node_2: Union[Path, str],
    pre_and_post_directory: Union[Path, str],
) -> None:
    """
    Postprocess after node 2.

    Parameters
    ----------
    bout_run_directory_node_2 : Path or str
        Directory where the dump files of the BOUT++ run of node 2 is stored
    pre_and_post_directory : Path or str
        Path where temporary files are stored to sign that nodes has been
        successfully processed
    """
    # Node 2 completed before node 5
    assert Path(bout_run_directory_node_2).joinpath("BOUT.settings").is_file()
    with Path(pre_and_post_directory).joinpath("5.txt").open("w") as file:
        file.write("Complete")


def node_seven(
    bout_run_directory_node_2: Union[Path, str],
    bout_run_directory_node_9: Union[Path, str],
    pre_and_post_directory: Union[Path, str],
) -> None:
    """
    Postprocess after node 2, preprocess before node 9.

    Parameters
    ----------
    bout_run_directory_node_2 : Path or str
        Directory where the dump files of the BOUT++ run of node 2 is stored
    bout_run_directory_node_9 : Path or str
        Directory where the dump files of the BOUT++ run of node 9 is stored
    pre_and_post_directory : Path or str
        Path where temporary files are stored to sign that nodes has been
        successfully processed
    """
    # Node 2 completed before node 7
    assert Path(bout_run_directory_node_2).joinpath("BOUT.settings").is_file()

    # Node 9 not completed before node 7
    assert not Path(bout_run_directory_node_9).joinpath("BOUT.settings").is_file()

    with Path(pre_and_post_directory).joinpath("7.txt").open("w") as file:
        file.write("Complete")


def node_eight(
    bout_run_directory_node_4: Union[Path, str],
    bout_run_directory_node_6: Union[Path, str],
    pre_and_post_directory: Union[Path, str],
) -> None:
    """
    Postprocess after node 6 and 4, preprocess before node 10.

    Parameters
    ----------
    bout_run_directory_node_4 : Path or str
        Directory where the dump files of the BOUT++ run of node 4 is stored
    bout_run_directory_node_6 : Path or str
        Directory where the dump files of the BOUT++ run of node 6 is stored
    pre_and_post_directory : Path or str
        Path where temporary files are stored to sign that nodes has been
        successfully processed
    """
    # Node 4 completed before node 8
    assert Path(bout_run_directory_node_4).joinpath("BOUT.settings").is_file()

    # Node 6 completed before node 8
    assert Path(bout_run_directory_node_6).joinpath("BOUT.settings").is_file()

    # Node 8 not completed before node 10
    assert not Path(pre_and_post_directory).joinpath("10.txt").is_file()

    with Path(pre_and_post_directory).joinpath("8.txt").open("w") as file:
        file.write("Complete")


def node_ten(
    bout_run_directory_node_9: Union[Path, str],
    pre_and_post_directory: Union[Path, str],
) -> None:
    """
    Postprocess after node 9 and 8.

    Parameters
    ----------
    bout_run_directory_node_9 : Path or str
        Directory where the dump files of the BOUT++ run of node 6 is stored
    pre_and_post_directory : Path or str
        Path where temporary files are stored to sign that nodes has been
        successfully processed
    """
    # Node 9 completed before node 10
    assert Path(bout_run_directory_node_9).joinpath("BOUT.settings").is_file()

    # Node 8 completed before node 10
    assert Path(pre_and_post_directory).joinpath("8.txt").is_file()

    with Path(pre_and_post_directory).joinpath("10.txt").open("w") as file:
        file.write("Complete")
