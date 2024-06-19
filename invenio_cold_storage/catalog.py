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
        self,
        record_id,
        operation,
        filename,
        new_qos=None,
        new_filename=None,
        dataset=None,
    ):
        #        record = RecordsBuckets.query.filter_by(record_id=record_id)
        #        print(record)
        # TODO: this should update the DB, and then reindex... for the time being, only updating the search (which I know)
        record_metadata = self._get_record(record_id)
        record = record_metadata["_source"].to_dict()
        found = False
        #print("READY TO UPDATE THE RECORD", record, filename, new_filename, dataset)

        for f in record["files"]:
            if dataset != f["key"]:
                continue
            if operation == "add":
                if new_qos == "cold":
                    new_field = "uri_cold"
                    old_field = "uri"
                else:
                    new_field = "uri"
                    old_field = "uri_cold"
                if old_field in f and f[old_field] == filename:
                    f[new_field] = new_filename
                    found = True
                elif "dataset" in f:
                    print(f"Adding a {new_qos} to a dataset")
                    for entry in f["dataset"]:
                        if old_field in entry and entry[old_field] == filename:
                            entry[new_field] = new_filename
                            found = True
            elif operation == "delete":
                if "uri" in f and f["uri"] == filename:
                    del f["uri"]
                    found = True
                elif "dataset" in f:
                    for entry in f["dataset"]:
                        if "uri" in entry and entry["uri"] == filename:
                            del entry["uri"]
                            found = True
                            break
            else:
                print(f"I do not understand the operation {operation}")
                return

        if not found:
            print("That file does not belong to that record")
            return
        self._search_client.update(
            index="records-record-v1.0.0",
            id=record_metadata["_id"],
            body={"doc": record},
            refresh=True,
        )
        print("Record updated")
        return True
