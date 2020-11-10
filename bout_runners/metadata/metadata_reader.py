"""Module containing the MetadataReader class."""


import logging
import re
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_reader import DatabaseReader
from pandas import DataFrame


def drop_ids(func: Callable) -> Callable:
    """
    Return a function which remove excessive ids.

    Parameters
    ----------
    func : function
        A function returning a DataFrame

    Returns
    -------
    drop : function
        The function dropping the ids
    """

    def drop(self, *args, **kwargs) -> DataFrame:
        """
        Drop columns inplace.

        Parameters
        ----------
        self : object
            Self reference to the instance the function is belonging to
            Must contain self.drop_id
        args : tuple
            Arguments belonging to the input function
        kwargs : dict
            Keyword arguments to the input function

        Returns
        -------
        data_frame : DataFrame
            The DataFrame where the ids has been dropped
        """
        data_frame = func(self, *args, **kwargs)
        columns = tuple(data_frame.columns)
        drop_columns = list()

        if self.drop_id == "parameters":
            drop_columns = [col for col in columns if col.startswith("parameters.")]

        elif self.drop_id == "keep_run_id":
            drop_columns = [
                col
                for col in columns
                if (col.endswith(".id") and not col == "run.id") or col.endswith("_id")
            ]

        elif self.drop_id == "all_id":
            drop_columns = [
                col for col in columns if col.endswith(".id") or col.endswith("_id")
            ]

        if self.drop_id is not None:
            data_frame.drop(drop_columns, axis=1, inplace=True)

        return data_frame

    return drop


class MetadataReader:
    r"""
    Class for reading the metadata from the database.

    Attributes
    ----------
    __db_reader : DatabaseConnector
        The connection to the database
    __table_names : tuple
        Getter variable for table_names
    __table_column_dict : dict of tuple
        Getter variable for table_column_dict
    __table_connections : dict of tuple
        Getter variable for table_connections
    __sorted_columns : tuple
        Getter variable for sorted_columns
    table_names : tuple
         A tuple containing all names of the tables
    table_column_dict : dict of tuple
        A dict where the keys are table names, and the values are corresponding
        column names
    table_connections : dict of tuple
        A dict where the keys are tables, and the values are tuples of tables
        connected to the key table
    sorted_columns : tuple
        A tuple of the column names as they will be sorted in the all_metadata DataFrame
    date_columns : tuple
        Columns containing dates
    drop_id : None or str
        Specifies what id columns should be dropped when obtaining the metadata

    Methods
    -------
    get_all_metadata()
        Return all of the run metadata
    get_parameters_metadata()
        Return only the parameter part of the run metadata
    get_join_query(from_statement, columns, alias_columns, table_connections)
        Return the query string of a `SELECT` query with `INNER JOIN`
    __get_parameters_query()
        Return the parameters query string
    __get_sorted_columns()
        Return all columns sorted
    __get_table_connections()
        Return a dict containing the table connections
    __get_all_table_names()
        Return all the table names in the schema
    __get_table_column_dict()
        Return all the column names of the specified tables

    Examples
    --------
    >>> from pathlib import Path
    >>> from bout_runners.database.database_connector import DatabaseConnector
    >>> db_connector = DatabaseConnector('test', Path())
    >>> metadata_reader = MetadataReader(db_connector)
    >>> metadata_reader.get_parameters_metadata()
       bar.id  bar.foo  ... parameters.baz_id  parameters.foo_id
    0       1        1  ...                 1                  1
    1       2       10  ...                 1                  2
    2       2       10  ...                 1                  1

    [3 rows x 16 columns]

    >>> metadata_reader.get_all_metadata()
       run.id  ...                  system_info.version
    0       1  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    1       2  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    2       3  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    3       4  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    4       5  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    5       6  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    6       7  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019

    [7 rows x 43 columns]

    >>> metadata_reader.drop_id = 'all_id'
    >>> metadata_reader.get_all_metadata()
      run.latest_status  ...                  system_info.version
    0          complete  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    1          complete  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    2          complete  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    3          complete  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    4             error  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    5           running  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019
    6         submitted  ...  #1 SMP Thu Oct 17 19:31:58 UTC 2019

    [7 rows x 28 columns]
    """

    date_columns = (
        "run.start_time",
        "run.stop_time",
        "run.submitted_time",
        "file_modification.bout_lib_modified",
        "file_modification.project_executable_modified",
        "file_modification.project_makefile_modified",
    )

    def __init__(
        self,
        db_connector: DatabaseConnector,
        drop_id: Optional[str] = "keep_run_id",
    ) -> None:
        """
        Set the database to use.

        Parameters
        ----------
        db_connector : DatabaseConnector
            The connection to the database
        drop_id : None or str
            Specifies what id columns should be dropped when obtaining the metadata
            - None : No columns will be dropped
            - 'parameters' : All columns containing parameters ids
              will be dropped
            - 'keep_run_id' : Only the run.id of the id columns will be
              kept
            - 'all_id' : All id columns will be removed
        """
        self.drop_id = drop_id

        self.__db_reader = DatabaseReader(db_connector)

        self.__table_names = self.__get_all_table_names()
        self.__table_column_dict = self.__get_table_column_dict()
        self.__table_connections = self.__get_table_connections()
        self.__sorted_columns = self.__get_sorted_columns()

        parameters_connections = {"parameters": self.__table_connections["parameters"]}
        parameters_tables = ("parameters", *parameters_connections["parameters"])
        self.__parameters_columns = tuple(
            str(col)
            for col in self.__sorted_columns
            if col.split(".")[0] in parameters_tables
        )

    @property
    def table_names(self) -> Tuple[str, ...]:
        """
        Set the properties of self.table_names.

        Returns
        -------
        self.__table_names : tuple
            A tuple containing all names of the tables
        """
        return self.__table_names

    @property
    def table_column_dict(
        self,
    ) -> Dict[str, Tuple[str, ...]]:
        """
        Set the properties of self.table_column_dict.

        Returns
        -------
        self.__table_column_dict : dict of tuple
            A dict where the keys are table names, and the values are corresponding
            column names
        """
        return self.__table_column_dict

    @property
    def table_connection(
        self,
    ) -> Dict[str, Tuple[str, ...]]:
        """
        Set the properties of self.table_connections.

        Returns
        -------
        self.__table_connections : dict of tuple
            A dict where the keys are tables, and the values are tuples of tables
            connected to the key table
        """
        return self.__table_connections

    @property
    def sorted_columns(self) -> Tuple[str, ...]:
        """
        Set the properties of self.sorted_columns.

        Returns
        -------
        self.__sorted_columns : tuple
            A tuple of the column names as they will be sorted in the all_metadata
            DataFrame
        """
        return self.__sorted_columns

    @drop_ids
    def get_all_metadata(self):
        """
        Return all of the run metadata.

        Returns
        -------
        DataFrame
            The DataFrame of the run metadata
        """
        parameters_query = self.__get_parameters_query()

        # Adding spaces and parenthesis
        parameter_sub_query = "\n".join(
            [f'{" " * 6}{line}' for line in parameters_query.split("\n")]
        )
        parameter_sub_query = (
            f"{parameter_sub_query[:5]}({parameter_sub_query[6:-1]}) " f"AS subquery"
        )

        # NOTE: The subquery names are the names of the columns after
        #       the query. We would like to rename them to
        #       sorted_columns. Hence the `columns` field and
        #       `alias_columns` field appears swapped
        subquery_columns = [
            f'subquery."{col}"' if col in self.__parameters_columns else col
            for col in self.sorted_columns
        ]
        # Remove the parameters from the table_connection to avoid
        # double joining
        table_connections = self.__table_connections.copy()
        table_connections.pop("parameters")
        unfinished_all_metadata_query = self.get_join_query(
            "run", subquery_columns, self.sorted_columns, table_connections
        )

        # Update the parameters columns
        all_metadata_query = unfinished_all_metadata_query.replace(
            " parameters ", f"\n{parameter_sub_query}\n"
        ).replace("= parameters.id", '= subquery."parameters.id"')

        return self.__db_reader.query(all_metadata_query, parse_dates=self.date_columns)

    @drop_ids
    def get_parameters_metadata(self):
        """
        Return only the parameter part of the run metadata.

        Returns
        -------
        DataFrame
            The DataFrame of the parameter metadata
        """
        parameters_query = self.__get_parameters_query()

        return self.__db_reader.query(parameters_query)

    @staticmethod
    def get_join_query(
        from_statement: str,
        columns: Sequence[str],
        alias_columns: Sequence[str],
        table_connections: Dict[str, Tuple[str, ...]],
    ) -> str:
        """
        Return the query string of a `SELECT` query with `INNER JOIN`.

        Notes
        -----
        The tables in `table_connection` is assumed to be joined by `id`s. I.e.
        `table_a` is connected to `table_b` by `table_b` having a column named
        `table_a_id` which corresponds to the `id` column of `table_a`

        Parameters
        ----------
        from_statement : str
            The statement after the `FROM` keyword in the query
            I.e.

            >>> f'SELECT * FROM {from_statement}'

        columns : array_like
            The columns to select from the tables
            I.e.

            >>> f'SELECT {columns} FROM *'

        alias_columns : array_like
            The name of the columns in the resulting table
            I.e.

            >>> f'SELECT {columns[0]} AS {alias_columns[0]} FROM *'

        table_connections : dict
            A dict where the keys are the table names, and the values are tuples
            containing table names connected to the key table as described in the
            note above

        Returns
        -------
        query : str
            The SQL-string which can be used to query where table in databases are
            joined through `INNER JOIN` operations
        """
        query = "SELECT\n"
        for column, alias in zip(columns, alias_columns):
            query += f'{" " * 7}{column} AS "{alias}",\n'
        # Remove last comma
        query = f"{query[:-2]}\n"
        query += f"FROM {from_statement}\n"
        for left_table in table_connections.keys():
            for right_table in table_connections[left_table]:
                query += (
                    f'{" " * 4}INNER JOIN {right_table} ON '
                    f"{left_table}."
                    f"{right_table}_id = {right_table}.id\n"
                )
        return query

    def __get_parameters_query(self) -> str:
        """
        Return the parameters query string.

        Returns
        -------
        parameters_query : str
            The SQL-string which can be used to query where table in databases are
            joined through `INNER JOIN` operations
        """
        parameter_connections = {"parameters": self.__table_connections["parameters"]}
        parameters_query = self.get_join_query(
            "parameters",
            self.__parameters_columns,
            self.__parameters_columns,
            parameter_connections,
        )
        return parameters_query

    def __get_sorted_columns(self) -> Tuple[str, ...]:
        """
        Return all columns sorted.

        The columns will be sorted alphabetically first by table name,
        then alphabetically by column name, with the following exceptions:

        1. The columns from the run table is presented first
        2. The id column is the first column in the table

        Returns
        -------
        tuple
            Dict containing the column names
            On the form

            >>> ('run.id',
            ...  'run.column_name_1',
            ...  'run.column_name_2',
            ...  ...
            ...  'table_name_1.column_name_1',
            ...  'table_name_1.column_name_2', ...)
        """
        sorted_columns: List[str] = list()
        table_names = sorted(self.table_column_dict.keys())
        table_names.pop(table_names.index("run"))
        table_names.insert(0, "run")
        for table_name in table_names:
            table_columns = list()
            for column_name in sorted(self.table_column_dict[table_name]):
                table_columns.append(f"{table_name}.{column_name}")
            table_columns.pop(table_columns.index(f"{table_name}.id"))
            table_columns.insert(0, f"{table_name}.id")
            sorted_columns = [*sorted_columns, *table_columns]
        return tuple(sorted_columns)

    def __get_table_connections(self) -> Dict[str, Tuple[str, ...]]:
        """
        Return a dict containing the table connections.

        Returns
        -------
        table_connection_dict : dict
            A dict telling which tables are connected to each other, where the key
            is the table under consideration and the value is a tuple containing the
            tables which have a key connection to the table under consideration
            On the form

            >>> {'table_1': ('table_2', 'table_3'),
            ...  'table_4': ('table_5',), ...}

        Raises
        ------
        RuntimeError
            If match is None
        """
        table_connection_dict = dict()
        pattern = re.compile("(.*)_id")

        for table, columns in self.table_column_dict.items():
            ids: List[str] = list()
            for column in columns:
                if "_id" in column:
                    match = pattern.match(column)
                    if match is None:
                        msg = f"match is None for '(.*)_id' for input '{column}'"
                        logging.critical(msg)
                        raise RuntimeError(msg)
                    ids.append(match[1])
            if len(ids) > 0:
                table_connection_dict[table] = tuple(ids)

        return table_connection_dict

    def __get_all_table_names(self) -> Tuple[str, ...]:
        """
        Return all the table names in the schema.

        Returns
        -------
        tuple
            A tuple containing all names of the tables
        """
        query = (
            "SELECT name FROM sqlite_master\n"
            "WHERE\n"
            "    type ='table' AND\n"
            "    name NOT LIKE 'sqlite_%'"
        )
        # pylint: disable=no-member
        return tuple(self.__db_reader.query(query).loc[:, "name"])

    def __get_table_column_dict(self) -> Dict[str, Tuple[str, ...]]:
        """
        Return all the column names of the specified tables.

        Returns
        -------
        table_column_dict : dict of tuple
            Dict containing the column names
            On the form

            >>> {'table_1': ('table_1_column_1', ...),
            ...  'table_2': ('table_2_column_1', ...), ...}
        """
        table_column_dict = dict()

        query = "SELECT name FROM pragma_table_info('{}')"

        for table_name in self.table_names:
            # pylint: disable=no-member
            table_column_dict[table_name] = tuple(
                self.__db_reader.query(query.format(table_name)).loc[:, "name"]
            )

        return table_column_dict
