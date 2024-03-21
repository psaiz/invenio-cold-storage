from time import time

from invenio_db import db
from invenio_indexer.api import RecordIndexer
from invenio_records_files.api import Record
from invenio_records_files.models import RecordsBuckets
from invenio_search import current_search_client
from invenio_search.engine import dsl, search
from invenio_search.utils import prefix_index


class Catalog:
    def __init__(self):
        self._search_client = current_search_client
        self._index = prefix_index("fts_persistency")
        self._indexer = RecordIndexer()

    def _get_record(self, record_id):
        try:
            rec_id = int(record_id)
        except ValueError:
            print(
                f"The recid '{record_id}' does not seem like a number. Could it be that it is not the correct id?"
            )
            return None
        try:
            results = (
                dsl.Search(
                    using=self._search_client,
                    index="records-record-v1.0.0",
                )
                .filter("term", recid=rec_id)
                .extra(size=1)
                .execute()
            )
            print("QUE TENEMOS")
            print(results)
            print(results.to_dict())
            if len(results["hits"]["hits"]):
                return results["hits"]["hits"][0]
            print(f"The record {record_id} is not indexed")
            return None
        except search.exceptions.NotFoundError:
            print(f"The record {record_id} does not exist")
            return None

    def get_files_from_record(self, record_id, files):
        record = self._get_record(record_id)
        if record:
            return record["_source"]["files"]
        return None

    def update_record(
        self, record_id, operation, filename, new_qos=None, new_filename=None
    ):
        #        record = RecordsBuckets.query.filter_by(record_id=record_id)
        #        print(record)
        # TODO: this should update the DB, and then reindex... for the time being, only updating the search (which I know)
        record_metadata = self._get_record(record_id)
        record = record_metadata["_source"].to_dict()
        found = False
        for f in record["files"]:
            if "uri" in f and f["uri"] == filename:
                if operation == "add" and new_qos == "cold":
                    f["uri_cold"] = new_filename
                elif operation == "delete":
                    if f["uri"] == filename:
                        print("Deleting the hot copy")
                        del f["uri"]
                else:
                    print(f"I do not understand the operation {operation}")
                    return
                found = True
                break
            elif "uri_cold" in f and f["uri_cold"] == filename:
                if operation == "add" and new_qos == "hot":
                    f["uri"] = new_filename
                    found = True
                    break
                print(f"I do not understand the operation {opration}")
                return

        if not found:
            print("THat file does not belong to that record")
            return
        self._search_client.update(
            index="records-record-v1.0.0",
            id=record_metadata["_id"],
            body={"doc": record},
        )
        self._search_client.indices.refresh(index="records-record-v1.0.0")
        print("Record updated")
