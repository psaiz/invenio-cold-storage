from time import time

from invenio_db import db
from invenio_records_files.api import Record
from invenio_records_files.models import RecordsBuckets
from invenio_search import current_search_client
from invenio_search.engine import dsl, search
from invenio_search.utils import prefix_index
from invenio_indexer.api import RecordIndexer

from invenio_files_rest.models import FileInstance, ObjectVersionTag, ObjectVersion
from invenio_db import db
from cernopendata.api import RecordFilesWithIndex
#from sqlalchemy.exc import IntegrityError
from datetime import datetime
class Catalog:
    def __init__(self, debug=False):
        self._search_client = current_search_client
        self._index = prefix_index("fts_persistency")
        self._indexer = RecordIndexer()
        self._debug = debug
        self._reindex_queue = []

    def get_record_uuid(self, recid):
        """First, lets get the record."""
        record = self._get_record(recid)
        if not record:
            return None
        return record["_id"]
    def _get_record(self, record_id):
        try:
            rec_id = int(record_id)
        except ValueError:
            print(
                f"The recid '{record_id}' does not seem like a number. Could it be that it is not the correct id?"
            )
            return None
        try:
            # TODO change the index name
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
        files = []
        if self._debug:
            print("DEBUG: The catalog got the record:", record.to_dict())
        if record:
            if "_files" in record["_source"]:
                files += record["_source"]["_files"]
            if "_file_indices" in record["_source"]:
                for f in record["_source"]["_file_indices"]:
                    files += f["files"]
        if self._debug:
            print("DEBUG: And the list of files are:", files)
        return files

    def clear_hot(self, record_uuid, file_id):
        """Marking the hot copy as deleted"""
        def _clear_hot_function(version_id):
            """HEHE"""
            ObjectVersionTag.create(version_id, "hot_deleted", str(datetime.now()))
        return self._update_file_and_reindex(record_uuid, file_id, _clear_hot_function)

    def _update_file_and_reindex(self, record_uuid, file_id, update_function):
        f= FileInstance.get(file_id)
        if not f:
            print(f"Can't find that file :( {file_id}")
            return False
        objectVersion= ObjectVersion.query.filter_by(file_id=f.id).one_or_none()
        if not objectVersion:
            print(f"Can't find the object associated to that file :( {file_id}")
            return False
        update_function(objectVersion.version_id)
        db.session.commit()
        if record_uuid not in self._reindex_queue:
            print(f"Record {record_uuid} will be reindexed")
            self._reindex_queue += [record_uuid]
        return True

    def reindex_entries(self):


        while  len(self._reindex_queue)>0:
            record_uuid = self._reindex_queue.pop(0)
            print(f"Ready to reindex {record_uuid}")
            record = RecordFilesWithIndex.get_record(record_uuid)
            if not record:
                print(f"Couldn't find that record '{record_uuid}'")
                continue
            print("Got the object from the database")
            record.files.flush()
            self._indexer.index(record)

    def add_copy(self, record_uuid, file_id, new_qos, new_filename):
        """Adds a copy to a particular file. It reindexes the record."""
        def _add_copy_function(version_id):
            """HELLO"""
            if new_qos=="cold":
                ObjectVersionTag.create_or_update(version_id, "uri_cold", new_filename)
            elif new_qos=="hot":
                ObjectVersionTag.delete(version_id, "hot_deleted")

        return self._update_file_and_reindex(record_uuid, file_id, _add_copy_function)

    def update_record(
        self,
        record_id,
        operation,
        filename,
        new_qos=None,
        new_filename=None,
        #dataset=None,
    ):

        my_file = FileInstance()
        print(my_file)
        print("GOT THE FILE!!!")
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
                    print("NOT DONE YET")
                    return False
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
