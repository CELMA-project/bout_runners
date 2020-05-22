"""Contains the processor split class."""


import logging
from typing import Optional


class ProcessorSplit:
    """
    Class which sets the processor split.

    Attributes
    ----------
    __number_of_processors : None or int
        Getter and setter variable for number_of_processors
    __number_of_nodes : None or int
        Getter and setter variable for number_of_nodes
    __processors_per_node : None or int
        Getter and setter variable for processors_per_node
    number_of_processors : int
        The total number of processors to use
    number_of_nodes : int
        How many nodes to run on (only effective on clusters)
    processors_per_node : int
        The number of processors to allocate per node
        (only effective on clusters)

    Examples
    --------
    >>> processor_split = ProcessorSplit(number_of_processors=1,
    ...                                  number_of_nodes=1,
    ...                                  processors_per_node=1)

    >>> processor_split = ProcessorSplit(number_of_processors=2,
    ...                                  number_of_nodes=1,
    ...                                  processors_per_node=1)
    Traceback (most recent call last):
      File "<input>", line 1, in <module>
    ValueError: number_of_nodes*processors_per_node = 1, whereas
    number_of_processors = 2
    """

    def __init__(
        self,
        number_of_processors: int = 1,
        number_of_nodes: int = 1,
        processors_per_node: int = 1,
    ) -> None:
        """
        Set the parameters.

        Parameters
        ----------
        number_of_processors : int
            The total number of processors to use
        number_of_nodes : int
            How many nodes to run on (only effective on clusters)
        processors_per_node : int
            The number of processors to allocate per node
            (only effective on clusters)
        """
        # Declare variables to be used in the getters and setters
        self.__number_of_processors: Optional[int] = None
        self.__number_of_nodes: Optional[int] = None
        self.__processors_per_node: Optional[int] = None

        # Set the number of processors
        self.number_of_processors = number_of_processors

        # Set the number of nodes
        self.number_of_nodes = number_of_nodes

        # Set the processors per node
        self.processors_per_node = processors_per_node

    @property
    def number_of_processors(self) -> int:
        """
        Set the properties of self.number_of_processors.

        Returns
        -------
        int
            The number of processors
        """
        return self.__number_of_processors

    @number_of_processors.setter
    def number_of_processors(self, number_of_processors: int):
        self.__number_of_processors = number_of_processors
        if self.number_of_nodes is not None and self.processors_per_node is not None:
            self.__enough_nodes_check()

        logging.debug("number_of_processors set to %s", number_of_processors)

    @property
    def number_of_nodes(self) -> int:
        """
        Set the properties of self.number_of_nodes.

        Returns
        -------
        int
            The number of nodes
        """
        return self.__number_of_nodes

    @number_of_nodes.setter
    def number_of_nodes(self, number_of_nodes: int):
        self.__number_of_nodes = number_of_nodes
        if (
            self.number_of_processors is not None
            and self.processors_per_node is not None
        ):
            self.__enough_nodes_check()
        logging.debug("number_of_nodes set to %s", number_of_nodes)

    @property
    def processors_per_node(self) -> int:
        """
        Set the properties of self.processors_per_node.

        Returns
        -------
        int
            The number of nodes
        """
        return self.__processors_per_node

    @processors_per_node.setter
    def processors_per_node(self, processors_per_node: int):
        self.__processors_per_node = processors_per_node
        if self.number_of_processors is not None and self.number_of_nodes is not None:
            self.__enough_nodes_check()
        logging.debug("processors_per_node set to %s", processors_per_node)

    def __enough_nodes_check(self) -> None:
        """
        Check that enough nodes are allocated.

        Raises
        ------
        ValueError
            If
            self.number_of_nodes * self.processors_per_node < self.number_of_processors
        """
        product = self.number_of_nodes * self.processors_per_node
        if product < self.number_of_processors:
            msg = (
                f"number_of_nodes*processors_per_node = {product}, "
                f"whereas number_of_processors = "
                f"{self.number_of_processors}"
            )
            raise ValueError(msg)
