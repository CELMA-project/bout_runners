"""
Functions to test a complex network.

To visualize the network, run the following code, and paste the content to
http://www.webgraphviz.com

>>> import networkx as nx
>>> g = nx.DiGraph()
...
>>> for i in range(4):
>>>     g.add_node(i, status="ready")
...
>>> g.add_edge(0, 2)
>>> g.add_edge(2, 3)
>>> g.add_edge(1, 3)
...
>>> print(nx.nx_pydot.to_pydot(g))
"""

from typing import Union
from pathlib import Path


def node_zero(
    save_dir: Union[Path, str],
) -> None:
    """
    Assert that that node dependencies are correct, and write a txt file.

    Parameters
    ----------
    save_dir : Path or str
        Path to store .txt files
    """
    assert not Path(save_dir).joinpath("2.txt").is_file()
    assert not Path(save_dir).joinpath("3.txt").is_file()
    with Path(save_dir).joinpath("0.txt").open("w") as file:
        file.write("Complete")


def node_one(
    save_dir: Union[Path, str],
) -> None:
    """
    Assert that that node dependencies are correct, and write a txt file.

    Parameters
    ----------
    save_dir : Path or str
        Path to store .txt files
    """
    assert not Path(save_dir).joinpath("2.txt").is_file()
    assert not Path(save_dir).joinpath("3.txt").is_file()
    with Path(save_dir).joinpath("1.txt").open("w") as file:
        file.write("Complete")


def node_two(
    save_dir: Union[Path, str],
) -> None:
    """
    Assert that that node dependencies are correct, and write a txt file.

    Parameters
    ----------
    save_dir : Path or str
        Path to store .txt files
    """
    assert Path(save_dir).joinpath("0.txt").is_file()
    assert not Path(save_dir).joinpath("3.txt").is_file()
    with Path(save_dir).joinpath("2.txt").open("w") as file:
        file.write("Complete")


def node_three(
    save_dir: Union[Path, str],
) -> None:
    """
    Assert that that node dependencies are correct, and write a txt file.

    Parameters
    ----------
    save_dir : Path or str
        Path to store .txt files
    """
    assert Path(save_dir).joinpath("0.txt").is_file()
    assert Path(save_dir).joinpath("1.txt").is_file()
    assert Path(save_dir).joinpath("2.txt").is_file()
    with Path(save_dir).joinpath("3.txt").open("w") as file:
        file.write("Complete")
