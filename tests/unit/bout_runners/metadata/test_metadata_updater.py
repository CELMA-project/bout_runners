"""Contains unittests for the MetadataUpdater."""


from datetime import datetime


def test_update_start_time(get_metadata_updater_and_db_reader):
    """
    Test if it's possible to update the start time.

    Parameters
    ----------
    get_metadata_updater_and_db_reader : function
        Function which returns the MetadataUpdater object with
        initialized with connection to the database and a
        corresponding DatabaseReader object
    """
    metadata_updater, db_reader = get_metadata_updater_and_db_reader("start_time")
    now = datetime.now()
    run_id = metadata_updater.run_id
    metadata_updater.update_start_time(now)
    result_df = db_reader.query(f"SELECT start_time FROM run WHERE id = {run_id}")
    assert result_df.loc[0, "start_time"] == str(now)


def test_update_stop_time(get_metadata_updater_and_db_reader):
    """
    Test if it's possible to update the stop time.

    Parameters
    ----------
    get_metadata_updater_and_db_reader : function
        Function which returns the MetadataUpdater object with
        initialized with connection to the database and a
        corresponding DatabaseReader object
    """
    metadata_updater, db_reader = get_metadata_updater_and_db_reader("stop_time")
    now = datetime.now()
    run_id = metadata_updater.run_id
    metadata_updater.update_stop_time(now)
    result_df = db_reader.query(f"SELECT stop_time FROM run WHERE id = {run_id}")
    assert result_df.loc[0, "stop_time"] == str(now)


def test_update_latest_status(get_metadata_updater_and_db_reader):
    """
    Test if it's possible to update the latest status.

    Parameters
    ----------
    get_metadata_updater_and_db_reader : function
        Function which returns the MetadataUpdater object with
        initialized with connection to the database and a
        corresponding DatabaseReader object
    """
    metadata_updater, db_reader = get_metadata_updater_and_db_reader("latest_status")
    latest_status = "foobar"
    run_id = metadata_updater.run_id
    metadata_updater.update_latest_status(latest_status)
    result_df = db_reader.query(f"SELECT latest_status FROM run WHERE id = {run_id}")
    assert result_df.loc[0, "latest_status"] == latest_status
