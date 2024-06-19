from .catalog import Catalog
from .persistency import Persistency
from .storage import Storage
from os import getpid
import time
class ColdStorageManager:
    def __init__(self, app=None, debug=False):
        self._catalog = Catalog(debug=debug)
        self._persistency = Persistency()
        self._storage = Storage(app)
        self._counter= 0

    def _is_qos(self, file, qos):
        print(f"  Checking if the file {file['key']} is already {qos}...", end="")
        if qos == "cold":
            return "tags" in file and "uri_cold" in file["tags"]
        return not "tags" in file or not "hot_deleted" in file["tags"]

    def _move_record_entry(self, record_uuid, file, qos, move_function, register, check_exists):

        if self._is_qos(file, qos):
            print(f" it is already {qos}")
            return []

        if self._persistency.is_scheduled(file, qos):
            print("It is already scheduled")
            return []
        dest_file, transfer = self._storage.find_cold_url(file["uri"])
        if not dest_file:
            print(f"I can't find the cold url for {file['uri']}")
            return []
        if check_exists:
            exists = transfer.exists_file(dest_file)
            if exists:
                if register:
                    if file['size'] == exists["size"] and file['checksum'] == f"adler32:{exists['checksum']}":
                        print("It exists, and has the same size and checksum. Registering it")
                        self._counter +=1
                        return [self._persistency.insert_transfer({"id":f"manager_{getpid()}_{self._counter}", "record_uuid":record_uuid, "key":file['key'], "file_id":file['file_id'], "transfer":"invenio_cold_storage.transfer.cp", "new_qos":qos, "new_filename":dest_file})]
                print("The file already exists in the destination storage... Should it be registered (hint: `--register`)?")
                return []
        #print("WE SHOULD MOVE IT")
        entry = move_function(file)
        if not entry:
            print("Let's return without storing it")
            return []
        entry["record_uuid"] = record_uuid
        entry["key"] = file["key"]
        entry["file_id"] = file["file_id"]

        return [self._persistency.insert_transfer(entry)]


    def _move_record(self, recid, files, qos, move_function, register, check_exists):
        # Let's find the files inside the record
        transfers = []
        # Get he uuid of the record
        record_uuid = self._catalog.get_record_uuid(recid)
        if not record_uuid:
            return []
        for file in self._catalog.get_files_from_record(recid, files):
            transfers += self._move_record_entry(record_uuid, file, qos, move_function, register, check_exists)
        print(f"{len(transfers)} transfers have been issued")
        return transfers

    def archive(self, recid, files, register, check_exists):
        return self._move_record(recid, files, "cold", self._storage.archive, register, check_exists)

    def stage(self, record_id, files, register, check_exists):
        return self._move_record(record_id, files, "hot", self._storage.stage, register, check_exists)

    def clear_hot(self, record_id, files):
        # Let's find the files inside the record
        cleared = False
        for file in self._catalog.get_files_from_record(record_id, files):
            if not self._is_qos(file, "cold"):
                print("I don't want to remove the hot copy, since the cold does not exist!")
                continue
            print(" the file is cold and ", end="")
            if not self._is_qos(file, "hot"):
                print("the file is not in hot. Ignoring it")
                continue
            print(f"ready to be deleted")
            self._storage.clear_hot(file["uri"])
            record_uuid= self._catalog.get_record_uuid(record_id)
            self._catalog.clear_hot(record_uuid, file["file_id"])
            cleared = True
        return cleared

    def check_current_transfers(self):
        print("Checking all the ongoing transfers")
        now=int(time.time()*1000)
        all_status = {}
        summary = {}
        while True:
            # Let's do it in batches of 1k.
            transfers = self._persistency.get_transfers(now, 1000)
            if not transfers:
                break
            for transfer in transfers:
                id=transfer['id']
                print(f"Transfer {id}:", end="")
                status, error = self._storage._transfers[transfer["transfer"]].transfer_status(id)
                all_status[id] = status
                final =False
                if status not in summary:
                    summary[status] =0
                summary[status] += 1
                if status == "DONE":
                    print(" just finished! Let's update the catalog and mark it as done")
                    if self._catalog.add_copy(
                        transfer["record_uuid"],
                        transfer["file_id"],
                        transfer["new_qos"],
                        transfer["new_filename"],
                    ):
                        final = True
                if status == "FAILED":
                    print("The transfer failed :(")
                    final =True
                else:
                    print(f" status {status}")
                self._persistency.update_transfer(id, status, final, error)
        self._catalog.reindex_entries()
        print("Summary: ", summary)
        return all_status

    def settings(self):
        """Return the configuration of the cold_storage"""
        return "Storing settings: " + self._storage.settings()

    def list(self, record, files):
        """Returns the location of the files for a particular record"""
        return self._catalog.get_files_from_record(record, files)
