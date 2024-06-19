from time import time

from invenio_search import current_search_client
from invenio_search.engine import search
from invenio_search.utils import prefix_index


class Persistency:
    def __init__(self):
        self._search_client = current_search_client
        self._index = prefix_index("cold-persistency")

    def insert_transfer(self, entry):
        now = int(time() * 1000)
        self._search_client.index(
            index=self._index,
            id=entry["id"],
            body={
                "new_qos": entry["new_qos"],
                "new_filename": entry["new_filename"],
                "record_uuid": entry["record_uuid"],
                "file_id": entry["file_id"],
                "submitted": now,
                "transfer":entry["transfer"],
                "last_check":now,
            },
            refresh=True,
        )
        return entry["id"]

    def update_transfer(self, id,  status, finished, reason=None):
        now = int(time() * 1000)
        update = {"last_check": now, "status":status}
        if finished:
            update['ack'] = now
        if reason:
            update["reason"] = reason
        self._search_client.update(
            self._index, id, body={"doc": update }, refresh=True
        )

    def get_transfers(self, last_check, size=1000):
        ids = []

        result = self._search_client.search(
            index=self._index,
            _source=["record_uuid", "file_id", "new_qos", "new_filename", "transfer"],
            body={"query": {"bool": { "must": [{"range":{"last_check":{"lte":last_check}}}],
                "must_not": [{"exists": {"field": "ack"}}]}}},
            sort="last_check",
            size=size,
        )
        for doc in result["hits"]["hits"]:
            ids.append({"id":doc["_id"],
                        "record_uuid": doc["_source"]["record_uuid"],
                        "file_id": doc["_source"]["file_id"],
                        "new_qos": doc["_source"]["new_qos"],
                        "new_filename": doc["_source"]["new_filename"],
                        "transfer": doc["_source"]["transfer"],
                        })
        return ids

    def get_transfer_details(self, id):
        return self._search_client.get(index=self._index, id=id)

    def is_scheduled(self, file, qos):
        try:
            result = self._search_client.count(
                index=self._index,
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"new_qos": {"value": qos}}},
                                {"term": {"file_id": {"value": file["file_id"]}}},
                            ],
                            "must_not": [{"exists": {"field": "ack"}}],
                        }
                    }
                },
            )
        except search.exceptions.NotFoundError:
            print(
                "There is not even an index for the transfers... nothing is scheduled"
            )
            return False
        except search.exceptions.TransportError:
            print("Error connecting to opensearch! Just in case, assume that there is a transfer ")
            return True
        return result["count"] > 0
