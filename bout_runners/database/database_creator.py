"""Module containing the DatabaseCreator class."""


import logging
import re
from typing import Dict, Optional, Tuple, List

from bout_runners.database.database_connector import DatabaseConnector
from bout_runners.database.database_utils import get_system_info_as_sql_type


class DatabaseCreator:
    r"""
    Class for creating the schema of the database.

    Attributes
    ----------
    db_connector : DatabaseConnector
        The database object to write to

    Methods
    -------
    create_all_schema_tables(parameters_as_sql_types)
        Create the all the tables for a schema
    get_create_table_statement(table_name, columns=None,
    primary_key='id', foreign_keys=None)
        Return a SQL string which can be used to create the table
    _create_single_table(table_str)
        Create a table in the database
    _create_system_info_table()
        Create a table for the system info
    _create_split_table()
        Create a table which stores the grid split
    _create_file_modification_table()
        Create a table for file modifications
    _create_parameter_tables(parameters_as_sql_types)
        Create a table for each BOUT.settings section and a join table
    _create_run_table()
        Create a table for the metadata_recorder of a run

    Examples
    --------
    Import dependencies

    >>> from pathlib import Path
    >>> from bout_runners.executor.bout_paths import BoutPaths
    >>> from bout_runners.parameters.default_parameters import DefaultParameters
    >>> from bout_runners.parameters.final_parameters import FinalParameters
    >>> from bout_runners.database.database_connector import DatabaseConnector

    Create the `bout_paths` object

    >>> project_path = Path().joinpath('path', 'to', 'project')
    >>> bout_inp_src_dir = Path().joinpath('path', 'to', 'source', 'BOUT.inp')
    >>> bout_inp_dst_dir = Path().joinpath('path', 'to', 'destination', 'BOUT.inp')
    >>> bout_paths = BoutPaths(project_path=project_path,
    ...                        bout_inp_src_dir=bout_inp_src_dir,
    ...                        bout_inp_dst_dir=bout_inp_dst_dir)

    Obtain the parameters

    >>> default_parameters = DefaultParameters(bout_paths)
    >>> final_parameters = FinalParameters(default_parameters)
    >>> final_parameters_dict = final_parameters.get_final_parameters()
    >>> final_parameters_as_sql_types = \
    ...     final_parameters.cast_to_sql_type(final_parameters_dict)

    Create the database

    >>> db_connector = DatabaseConnector('name', project_path)
    >>> db_creator = DatabaseCreator(db_connector)
    >>> db_creator.create_all_schema_tables(final_parameters_as_sql_types)
    """

    def __init__(self, db_connector: DatabaseConnector) -> None:
        """
        Set the database to use.

        Parameters
        ----------
        db_connector : DatabaseConnector
            The database object to write to
        """
        self.db_connector = db_connector

    def create_all_schema_tables(
        self, parameters_as_sql_types: Dict[str, Dict[str, str]]
    ) -> None:
        """
        Create the all the tables for a schema.

        The database schema will be on normalized form, see [1]_ for a quick
        overview, and [2]_ for a slightly deeper explanation

        Parameters
        ----------
        parameters_as_sql_types : dict
            The dictionary on the form

            >>> {'section': {'parameter': 'value_type'}}

        References
        ----------
        .. [1] https://www.databasestar.com/database-normalization/
        .. [2] http://www.bkent.net/Doc/simple5.htm
        """
        # Check if tables are created
        tables = list()
        tables.append(self._create_system_info_table())
        tables.append(self._create_split_table())
        tables.append(self._create_file_modification_table())
        tables.extend(self._create_parameter_tables(parameters_as_sql_types))
        tables.append(self._create_run_table())

        logging.info(
            "Created the following tables in %s: %s", self.db_connector.db_path, tables
        )

    @staticmethod
    def get_create_table_statement(
        table_name: str,
        columns: Optional[Dict[str, str]] = None,
        primary_key: str = "id",
        foreign_keys: Optional[Dict[str, Tuple[str, str]]] = None,
    ) -> str:
        """
        Return a SQL string which can be used to create the table.

        Parameters
        ----------
        table_name : str
            Name of the table
        columns : dict or None
            Dictionary where the key is the column name and the value is
            the type
        primary_key : str
            Name of the primary key (the type is set to INTEGER)
        foreign_keys : dict or None
            Dictionary where the key is the column in this table to be used as a
            foreign key and the value is the tuple consisting of (name_of_the_table,
            key_in_table) to refer to

        Returns
        -------
        create_statement : str
            The SQL statement which creates table
        """
        create_statement = f"CREATE TABLE {table_name} \n("

        create_statement += f"   {primary_key} INTEGER PRIMARY KEY,\n"

        # These are not known during submission time
        nullable_fields = ("start_time", "stop_time")
        if columns is not None:
            for name, sql_type in columns.items():
                create_statement += f"    {name} {sql_type}"
                if name in nullable_fields:
                    nullable_str = ",\n"
                else:
                    nullable_str = " NOT NULL,\n"
                create_statement += nullable_str

        if foreign_keys is not None:
            # Create the key as column
            # NOTE: All keys are integers
            for name in foreign_keys.keys():
                create_statement += f"    {name} INTEGER NOT NULL,\n"

            # Add constraint
            for name, (f_table_name, key_in_table) in foreign_keys.items():
                # If the parent is updated or deleted, we would like the
                # same effect to apply to its child (thereby the CASCADE
                # parameter)
                create_statement += (
                    f"    FOREIGN KEY({name}) \n"
                    f"        REFERENCES {f_table_name}"
                    f"({key_in_table})\n"
                    f"            ON UPDATE CASCADE\n"
                    f"            ON DELETE CASCADE,"
                    f"\n"
                )

        # Replace last comma with )
        create_statement = f"{create_statement[:-2]})"

        return create_statement

    def _create_single_table(self, table_str: str) -> str:
        """
        Create a table in the database.

        Parameters
        ----------
        table_str : str
            The query to execute

        Returns
        -------
        table_name : str
            Name of the table

        Raises
        ------
        ValueError
            If the table_str is not understood
        """
        # Obtain the table name
        pattern = r"CREATE TABLE (\w*)"

        match = re.match(pattern, table_str)
        if match is None:
            raise ValueError(f'table_str "{table_str}" not understood')

        table_name = match.group(1)

        self.db_connector.execute_statement(table_str)

        return table_name

    def _create_system_info_table(self) -> str:
        """
        Create a table for the system info.

        Returns
        -------
        str
            Name of the table
        """
        sys_info_dict = get_system_info_as_sql_type()
        sys_info_statement = self.get_create_table_statement(
            table_name="system_info", columns=sys_info_dict
        )
        return self._create_single_table(sys_info_statement)

    def _create_split_table(self) -> str:
        """
        Create a table which stores the grid split.

        Returns
        -------
        str
            Name of the table
        """
        split_statement = self.get_create_table_statement(
            table_name="split",
            columns={
                "number_of_processors": "INTEGER",
                "number_of_nodes": "INTEGER",
                "processors_per_node": "INTEGER",
            },
        )
        return self._create_single_table(split_statement)

    def _create_file_modification_table(self) -> str:
        """
        Create a table for file modifications.

        Returns
        -------
        str
            Name of the table
        """
        file_modification_statement = self.get_create_table_statement(
            table_name="file_modification",
            columns={
                "project_makefile_modified": "TIMESTAMP",
                "project_executable_modified": "TIMESTAMP",
                "project_git_sha": "TEXT",
                "bout_lib_modified": "TIMESTAMP",
                "bout_git_sha": "TEXT",
            },
        )
        return self._create_single_table(file_modification_statement)

    def _create_parameter_tables(
        self, parameters_as_sql_types: Dict[str, Dict[str, str]]
    ) -> List[str]:
        """
        Create a table for each BOUT.settings section and a join table.

        Parameters
        ----------
        parameters_as_sql_types : dict
            The dictionary on the form

            >>> {'section': {'parameter': 'value_type'}}

        Returns
        -------
        tables : list of str
            Tuple of table names

        Notes
        -----
        All `:` will be replaced by `_` in the section names
        """
        parameters_foreign_keys = dict()
        tables = list()
        for section in parameters_as_sql_types.keys():
            # Replace bad characters for SQL
            section_name = section.replace(":", "_")
            # Generate foreign keys for the parameters table
            parameters_foreign_keys[f"{section_name}_id"] = (section_name, "id")

            columns = dict()
            for parameter, value_type in parameters_as_sql_types[section].items():
                # Generate the columns
                columns[parameter] = value_type

            # Creat the section table
            section_statement = self.get_create_table_statement(
                table_name=section_name, columns=columns
            )
            tables.append(self._create_single_table(section_statement))

        # Create the join table
        parameters_statement = self.get_create_table_statement(
            table_name="parameters", foreign_keys=parameters_foreign_keys
        )
        tables.append(self._create_single_table(parameters_statement))
        return tables

    def _create_run_table(self) -> str:
        """
        Create a table for the metadata of a run.

        Returns
        -------
        str
            Name of the table
        """
        run_statement = self.get_create_table_statement(
            table_name="run",
            columns={
                "name": "TEXT",
                "submitted_time": "TIMESTAMP",
                "start_time": "TIMESTAMP",
                "stop_time": "TIMESTAMP",
                "latest_status": "TEXT",
            },
            foreign_keys={
                "file_modification_id": ("file_modification", "id"),
                "split_id": ("split", "id"),
                "parameters_id": ("parameters", "id"),
                "system_info_id": ("system_info", "id"),
            },
        )
        return self._create_single_table(run_statement)
