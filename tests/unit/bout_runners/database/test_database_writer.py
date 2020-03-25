"""Contains unittests for the database writer."""


import numpy as np
from bout_runners.database.database_writer import DatabaseWriter
from bout_runners.database.database_reader import DatabaseReader


def test_database_writer(make_test_schema):
    """
    Test we can create write to the database schemas.

    Specifically this test that:
    1. We can write to the `split` table
    2. That only one record is made
    3. That the type is correct
    4. Check that the values are correct

    Parameters
    ----------
    make_test_schema : function
        Function returning the database connection with the schema
        created
    """
    db_connection = make_test_schema('write_test')
    db_reader = DatabaseReader(db_connection)

    db_writer = DatabaseWriter(db_connection)
    dummy_split_dict = {'number_of_processors': 1,
                        'nodes': 2,
                        'processors_per_node': 3}
    db_writer.create_entry('split', dummy_split_dict)

    table = db_reader.query('SELECT * FROM split')

    # Check that the shape is expected (note that one column is
    # assigned to the id)
    assert table.shape == (1, 4)

    # Check all the elements are the same
    # https://www.quora.com/How-do-you-check-if-all-elements-in-a-NumPy-array-are-the-same-in-Python-pandas
    values = table.dtypes.values
    assert (values == np.dtype('int64')).all()

    for key, value in dummy_split_dict.items():
        assert table.loc[0, key] == value
