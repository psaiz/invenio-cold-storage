from .catalog import Catalog
from .persistency import Persistency
from .storage import Storage


class ColdStorageManager:
    def __init__(self, app=None):
        self._catalog = Catalog()
        self._persistency = Persistency()
        self._storage = Storage(app)

    def _is_qos(self, file, qos):
        print(f"  Checking if the file {file['key']} is already {qos}...", end="")
        if qos == "cold":
            return "uri_cold" in file
        return "uri" in file

    def _move_record_entry(self, record_id, file, qos, move_function, entry=None):
        if entry:
            print("    We are looking at an entry of the dataset ", end="")
            if self._is_qos(entry, qos):
                print(f" already {qos}: ignoring it.")
                return

            if self._persistency.is_scheduled(entry, qos):
                print(f" already scheduled for {qos}: ignoring it.")
                return
        else:
            if self._is_qos(file, qos):
                print(f" already {qos}: ignoring it.")
                return

            if self._persistency.is_scheduled(file, qos):
                print(f" already scheduled for {qos}: ignoring it.")
                return

        entry = move_function(file)
        entry["record_id"] = record_id
        self._persistency.insert_transfer(entry)
        return entry["id"]

    def _move_record(self, record_id, files, qos, move_function):
        # Let's find the files inside the record
        files = self._catalog.get_files_from_record(record_id, files)
        if not files:
            return
        transfers = []
        for file in files:
            transfer_id = self._move_record_entry(record_id, file, qos, move_function)
            if transfer_id:
                transfers.append(transfer_id)
            if "dataset" in file:
                print(
                    f"  The file is a dataset. Checking also the entries of the dataset"
                )
                for entry in file["dataset"]:
                    self._move_record_entry(record_id, file, qos, move_function, entry)

        return transfers

    def archive(self, record_id, files):
        print("VAMOS A  ARCHIVAR ", record_id)
        return self._move_record(record_id, files, "cold", self._storage.archive)

    def stage(self, record_id, files):
        return self._move_record(record_id, files, "hot", self._storage.stage)

    def clear_hot(self, record_id, files):
        # Let's find the files inside the record
        cleared = False
        files = self._catalog.get_files_from_record(record_id, files)
        if not files:
            return False
        for file in files:
            if not self._is_qos(file, "cold"):
                print(
                    "I don't want to remove the hot copy, since the cold does not exist!"
                )
                continue
            print(" the file is cold and ", end="")
            if "uri" not in file:
                print("the file is not in hot. Ignoring it")
                continue
            print(f"ready to delete {file['uri']}")
            self._storage.clear_hot(file["uri"])
            self._catalog.update_record(record_id, "delete", file["uri"])
            cleared = True
        return cleared

    def check_current_transfers(self):
        print("Checking all the ongoing transfers")

        transfers = self._persistency.get_transfers()
        all_status = {}
        for transfer in transfers:
            print(f"Transfer {transfer}:", end="")
            status = self._storage._transfer[0].transfer_status(transfer)
            all_status[transfer] = status
            if status == "DONE":
                print(" just finished! Let's update the catalog and mark it as done")
                entry = self._persistency.get_transfer_details(transfer)
                self._catalog.update_record(
                    entry["_source"]["record_id"],
                    "add",
                    entry["_source"]["filename"],
                    entry["_source"]["new_qos"],
                    entry["_source"]["new_filename"],
                )
                self._persistency.ack_transfer(transfer)
            else:
                print(f" status {status}")
        return all_status

    def settings(self):
        """Return the configuration of the cold_storage"""
        return "Storing settings: " + self._storage.settings()

    def list(self, record, files):
        """Returns the location of the files for a particular record"""
        return self._catalog.get_files_from_record(record, files)
