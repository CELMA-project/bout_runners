"""Contains unittests for the database writer."""


from typing import Callable

import numpy as np
from bout_runners.database.database_reader import DatabaseReader
from bout_runners.database.database_writer import DatabaseWriter


def test_db_writer(make_test_schema: Callable) -> None:
    """
    Test we can create write to the database schemas.

    Specifically this test that:
    1. We can write to the `split` table
    2. That only one record is made
    3. That the type is correct
    4. Check that the values are correct
    5. Check that it's possible to update the values

    Parameters
    ----------
    make_test_schema : function
        Function returning the database connection with the schema
        created
    """
    db_connection, _ = make_test_schema("write_test")
    db_reader = DatabaseReader(db_connection)

    db_writer = DatabaseWriter(db_connection)
    table_name = "split"
    dummy_split_dict = {
        "number_of_processors": 41,
        "number_of_nodes": 42,
        "processors_per_node": 43,
    }
    db_writer.create_entry(table_name, dummy_split_dict)

    table = db_reader.query(f"SELECT * FROM {table_name}")

    # Check that the shape is expected (note that one column is
    # assigned to the id)
    assert table.shape == (1, 4)

    # Check all the elements are the same
    # https://www.quora.com/How-do-you-check-if-all-elements-in-a-NumPy-array-are-the-same-in-Python-pandas
    values = table.dtypes.values
    assert (values == np.dtype("int64")).all()

    for key, value in dummy_split_dict.items():
        assert table.loc[0, key] == value  # pylint: disable=no-member

    update_fields = ("number_of_processors", "number_of_nodes")
    search_condition = (
        f"processors_per_node = " f'{dummy_split_dict["processors_per_node"]}'
    )
    values = tuple(dummy_split_dict[field] - 10 for field in update_fields)
    db_writer.update(
        db_writer.create_update_string(update_fields, table_name, search_condition),
        values,
    )
    table = db_reader.query(f"SELECT * FROM {table_name}")
    for index, field in enumerate(update_fields):
        # pylint: disable=no-member
        assert table.loc[:, field].values[0] == values[index]
