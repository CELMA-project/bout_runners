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


def node_zero(directory: Path) -> None:
    """
    Preprocess before node 2.

    Parameters
    ----------
    directory : path
        Path to store temporary file which signs that this node has been processed
    """
    pass


def node_one(directory: Path) -> None:
    """
    Preprocess before node 2.

    Parameters
    ----------
    directory : path
        Path to store temporary file which signs that this node has been processed
    """
    pass


def node_five(directory: Path) -> None:
    """
    Postprocess after node 2.

    Parameters
    ----------
    directory : path
        Path to store temporary file which signs that this node has been processed
    """
    pass


def node_seven(directory: Path) -> None:
    """
    Postprocess after node 2, preprocess before node 9.

    Parameters
    ----------
    directory : path
        Path to store temporary file which signs that this node has been processed
    """
    pass


def node_eight(directory: Path) -> None:
    """
    Postprocess after node 6 and 8, preprocess before 10.

    Parameters
    ----------
    directory : path
        Path to store temporary file which signs that this node has been processed
    """
    pass


def node_ten(directory: Path) -> None:
    """
    Preprocess after node 8 and 9.

    Parameters
    ----------
    directory : path
        Path to store temporary file which signs that this node has been processed
    """
    pass
