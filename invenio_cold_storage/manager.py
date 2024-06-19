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

    def _move_record_entry(self, record_id, file, qos, move_function):
        transfers = []
        files_todo = [file]

        if "dataset" in file:
            print(f"  The file is a dataset. Checking also the entries of the dataset")
            files_todo += file["dataset"]

        for my_file in files_todo:
       #     print(f"    Checking the file {my_file['key']}...", end="")
            if self._is_qos(my_file, qos):
                print(f" it is already {qos}")
            elif self._persistency.is_scheduled(my_file, qos):
                print("It is already scheduled")
            else:
                #print("WE SHOULD MOVE IT")
                entry = move_function(my_file)
                entry["record_id"] = record_id
                entry["key"] = my_file["key"]
                entry["dataset"] = file["key"]
                transfers += [self._persistency.insert_transfer(entry)]
        return transfers

    def _move_record(self, record_id, files, qos, move_function):
        # Let's find the files inside the record
        transfers = []
        for file in self._catalog.get_files_from_record(record_id, files):
            transfers += self._move_record_entry(record_id, file, qos, move_function)

        return transfers

    def archive(self, record_id, files):
        return self._move_record(record_id, files, "cold", self._storage.archive)

    def stage(self, record_id, files):
        return self._move_record(record_id, files, "hot", self._storage.stage)

    def clear_hot(self, record_id, files):
        # Let's find the files inside the record
        cleared = False
        for file in self._catalog.get_files_from_record(record_id, files):
            files_todo = [file]

            if "dataset" in file:
                print(f"  The file is a dataset. Checking also the entries of the dataset")
                files_todo += file["dataset"]
            for my_file in files_todo:
                if not self._is_qos(my_file, "cold"):
                    print(
                        "I don't want to remove the hot copy, since the cold does not exist!"
                    )
                    continue
                print(" the file is cold and ", end="")
                if "uri" not in my_file:
                    print("the file is not in hot. Ignoring it")
                    continue
                print(f"ready to be deleted")
                self._storage.clear_hot(my_file["uri"])
                self._catalog.update_record(record_id, "delete", my_file["uri"], "deleted", my_file['key'], file['key'])
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
                if self._catalog.update_record(
                    entry["_source"]["record_id"],
                    "add",
                    entry["_source"]["filename"],
                    entry["_source"]["new_qos"],
                    entry["_source"]["new_filename"],
                    entry["_source"]["dataset"],
                ):
                    self._persistency.ack_transfer(transfer)
                # self._persistency.fail_transfer(transfer)
            else:
                print(f" status {status}")
        print("Returning", all_status)
        return all_status

    def settings(self):
        """Return the configuration of the cold_storage"""
        return "Storing settings: " + self._storage.settings()

    def list(self, record, files):
        """Returns the location of the files for a particular record"""
        return self._catalog.get_files_from_record(record, files)
