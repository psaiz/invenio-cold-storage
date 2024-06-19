import os
import uuid
from unittest.mock import patch

import pytest
from invenio_app.factory import create_api as _create_api
from invenio_files_rest.models import Location
from invenio_indexer.api import RecordIndexer
from invenio_records_files.api import Record

from invenio_cold_storage.manager import ColdStorageManager


@pytest.fixture(scope="module")
def create_app(instance_path, entry_points):
    """Application factory fixture."""
    return _create_api


def fake_validate_endpoint(self):
    return {"url": "test", "api": {"major": 2, "minor": 2, "patch": 3}}


@pytest.fixture()
def cold_storage_manager(database, search):
    try:
        # TODO: isn't search_clear fixture supposed to do this?
        search.indices.delete("cold-persistency")
        search.indices.delete("records-record-v1.0.0")
    except Exception as e:
        print("DELETING DIDN'T WORK", e)
        pass
    return ColdStorageManager()


@pytest.fixture()
def cold_storage_record(database, search):
    print("Creating a test record and calling the manager")
    filename = "/home/invenio/hot_cache/var/data/my_doc.txt"
    f = open(filename, "w")
    f.write("DD")
    f.close()
    data = {
        #   "$schema": "file:///records/cold_storage-v1.0.0.json",
        "title": "My test record",
        "files": [
            {
                "key": "mydoc.txt",
                "checksum": "adler32:c4f2ef5e",
                "size": 5,
                "uri": f"file://{filename}",
            },
        ],
        "category": {"primary": "single_entry", "secondary": "test"},
        "recid": 54321,
    }
    return _store_record(data, database, search)


def _store_record(data, database, search):
    rec_uuid = uuid.uuid4()
    name = "local"
    location = Location.get_by_name(name)
    if not location:
        location = Location(name=name, uri="var/data", default=True)
        database.session.add(location)
        database.session.commit()
    r = Record.create(data, id_=rec_uuid)

    database.session.commit()
    RecordIndexer().index_by_id(r.id)
    search.indices.refresh("*")
    print("The test record is", r)
    return r


@pytest.fixture()
def cold_storage_dataset(database, search):
    print("creating a dataset example")
    data = {
        "title": "My example dataset",
        "files": [],
        "category": {"primary": "dataset", "secondary": "test"},
        "recid": 1623,
    }
    return _store_record(data, database, search)


@pytest.fixture(scope="module")
def extra_entry_points():
    """Extra entry points to load the mock_module features."""
    return {
        "invenio_db.model": [
            "mock_module = mock_module.models",
        ],
        "invenio_jsonschemas.schemas": [
            "mock_module = mock_module.jsonschemas",
        ],
        "invenio_search.index_templates": [
            "records = mock_module.index_templates",
        ],
    }
