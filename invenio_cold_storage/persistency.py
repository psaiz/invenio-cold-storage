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
                "record_id": entry["record_id"],
                "filename": entry["filename"],
                "submitted": now,
            },
            refresh=True,
        )

    def ack_transfer(self, id):
        now = int(time() * 1000)
        self._search_client.update(
            self._index, id, body={"doc": {"ack": now}}, refresh=True
        )

    def get_transfers(self):
        ids = []
        result = self._search_client.search(
            index=self._index,
            _source=False,
            body={"query": {"bool": {"must_not": [{"exists": {"field": "ack"}}]}}},
        )
        for doc in result["hits"]["hits"]:
            ids.append(doc["_id"])
        return ids

    def get_transfer_details(self, id):
        return self._search_client.get(index=self._index, id=id)

    def is_scheduled(self, file, qos):
        if qos == "cold":
            filename = file["uri"]
        else:
            filename = file["uri_cold"]
        try:
            result = self._search_client.count(
                index=self._index,
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"new_qos": {"value": qos}}},
                                {"term": {"filename": {"value": filename}}},
                            ],
                            "must_not": [{"exists": {"field": "ack"}}],
                        }
                    }
                },
            )
        except search.exceptions.NotFoundError:
            print("There is not even an index for the transfers... nothing is scheduled")
            return False
        return result["count"] > 0
