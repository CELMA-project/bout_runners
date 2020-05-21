"""Contains unittests for the database reader."""


from bout_runners.database.db_reader import DatabaseReader


def test_db_reader(make_test_database, write_to_split):
    """
    Test we can create read from the database.

    Specifically this test that:
    1. We can make a query
    2. That an empty db has not been populated
    3. That a populated db has table entries
    4. Check that we can extract the id for a given set of values
       which exist
    5. Check that no id is returned if a given set of values is not
       found in the database
    6. That we can get the latest row id which has been written to

    Parameters
    ----------
    make_test_database : function
        Function which returns the database connection
    write_to_split : function
        Function returning the database connection where `split` has
        been populated
    """
    empty_db_connection = make_test_database("empty_read_test")
    empty_db_reader = DatabaseReader(empty_db_connection)

    # Check that we can make a query
    table = empty_db_reader.query("SELECT 1+1 AS col")
    assert table.loc[0, "col"] == 2  # pylint: disable=no-member

    # Check that the tables has not been created in an empty db
    assert not empty_db_reader.check_tables_created()

    db_connection = write_to_split("read_test")
    db_reader = DatabaseReader(db_connection)

    # Check that tables exist
    assert db_reader.check_tables_created()

    # As write_to_split writes to the split table, we can get the
    # written values with the following query
    table = db_reader.query("SELECT * FROM split")
    entries_dict = table.to_dict(orient="records")[0]

    # Remove the 'id'
    entries_dict.pop("id")

    row_id = db_reader.get_entry_id("split", entries_dict)
    assert row_id == 1

    # Modify entries_dict so that row_id returns None
    entries_dict[list(entries_dict.keys())[0]] += 1
    new_row_id = db_reader.get_entry_id("split", entries_dict)
    assert new_row_id is None

    # Assert that get_latest_row_id is working
    assert db_reader.get_latest_row_id() == 1
