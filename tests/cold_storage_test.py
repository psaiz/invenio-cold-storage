def test_connect(cold_storage_manager):
    print(cold_storage_manager)


def test_archive(cold_storage_manager, cold_storage_record, search):
    def copies(record_id):
        record = search.get(index="records-record-v1.0.0", id=record_id)
        copies = 0
        if "uri" in record["_source"]["files"][0]:
            copies += 1
        if "uri_cold" in record["_source"]["files"][0]:
            copies += 1
        return copies

    record_recid = cold_storage_record["recid"]
    print(f"Starting with the record {record_recid} in hot storage")
    archive = cold_storage_manager.archive(record_recid, files=[])
    assert len(archive) == 1

    transfers = cold_storage_manager.check_current_transfers()
    assert transfers == {archive[0]: "DONE"}
    # If we look at the file, we should see two copies
    assert copies(cold_storage_record.id) == 2
    # And, if we check the waiting transfers again, it should be empty
    transfers = cold_storage_manager.check_current_transfers()
    assert transfers == {}
    print("The record is archived. Archiving again should not do anything")
    # The file is archived, so archiving again should fail
    assert cold_storage_manager.archive(record_recid, files=[]) == []

    print("Staging should fail, since the file is still on the hot storage")
    assert cold_storage_manager.stage(record_recid, files=[]) == []

    print("Cleaning the hot copy should work")
    assert cold_storage_manager.clear_hot(record_recid, files=[])

    print("Cleaning again should fail")
    assert not cold_storage_manager.clear_hot(record_recid, files=[])

    # At this point, the file should have only 1 copy
    assert copies(cold_storage_record.id) == 1

    print("Finally, let's stage it back")
    stage = cold_storage_manager.stage(record_recid, files=[])
    assert len(stage) == 1

    transfers = cold_storage_manager.check_current_transfers()
    assert transfers == {stage[0]: "DONE"}

    assert copies(cold_storage_record.id) == 2


def test_dataset(cold_storage_manager, cold_storage_dataset):
    print("Checking if this thing works with a dataset")
