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
    print("Fixture for the manager")
    try:
        # TODO: isn't search_clear fixture supposed to do this?
        print("DELETING SSOME INDICES")
        search.indices.delete("cold-persistency")
        search.indices.delete("records-record-v1.0.0")
    except Exception as e:
        print("DELETING DIDN'T WORK", e)
        pass
    return ColdStorageManager()


@pytest.fixture()
def cold_storage_record(cold_storage_manager, database, search):
    print("Creating a test record and calling the manager")
    data = {
        #   "$schema": "file:///records/cold_storage-v1.0.0.json",
        "title": "My test record",
        "files": [
            {
                "key": "mydoc.txt",
                "checksum": "adler32:c4f2ef5e",
                "size": 5,
                "uri": "file:///cold/data/var/data/my_doc.txt",
            },
        ],
        "recid": 54321,
    }
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
    yield r


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
