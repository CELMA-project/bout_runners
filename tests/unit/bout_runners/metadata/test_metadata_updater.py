"""Contains unittests for the metadata_updater."""


from datetime import datetime


def test_update_start_time(get_metadata_updater_and_db_reader):
    """
    FIXME
    """
    metadata_updater, db_reader = \
        get_metadata_updater_and_db_reader('start_time')
    now = datetime.now()
    run_id = metadata_updater.run_id
    metadata_updater.update_start_time(now)
    result_df = db_reader.query(
        f'SELECT start_time FROM run WHERE id = {run_id}')
    assert result_df.loc[0, 'start_time'] == str(now)


def test_update_end_time(get_metadata_updater_and_db_reader):
    """
    FIXME: YOU ARE HERE
    """
    pass
