# For the time being, let's import this directly. It would be nice if it could be an interface
# from invenio_fts.manager import TransferManager
import os
import re

from pkg_resources import iter_entry_points

from invenio_cold_storage.transter.cp import TransferManager


def default_transfer():
    return TransferManager()


def default_cold_path():
    return "var/data_cold"


def default_hot_path():
    return "var/data"


class Storage:
    def __init__(self, app=None):
        if app:
            self._hot_path = []
            for entry_point in iter_entry_points(
                "invenio_cold_storage.storage", name="hot_path"
            ):
                self._hot_path.append(entry_point.load()())
            self._cold_path = []
            for entry_point in iter_entry_points(
                "invenio_cold_storage.storage", name="cold_path"
            ):
                self._cold_path.append(entry_point.load()())
            self._transfer = []
            for entry_point in iter_entry_points(
                "invenio_cold_storage.storage", name="transfer"
            ):
                self._transfer.append(entry_point.load()())
        else:
            self._cold_path = ["/opt/invenio/var/instance/data_cold"]
            self._transfer = [TransferManager()]
            self._hot_path = ["/cold/data"]

    def archive(self, file):
        print(f"READY TO MOVE THE FILE {file['key']}, with {file['uri']}")
        filename = file["uri"]
        print(f"SWAPPING {self._hot_path[0]} by {self._cold_path[0]}")
        dest_file = filename.replace(self._hot_path[0], self._cold_path[0])
        return {
            "id": self._transfer[0].archive(filename, dest_file),
            "new_qos": "cold",
            "new_filename": dest_file,
            "filename": filename,
        }

    def stage(self, file):
        filename = file["uri_cold"]
        dest_file = filename.replace(self._cold_path[0], self._hot_path[0])
        print(f" Staging into {dest_file}")
        return {
            "id": self._transfer[0].stage(filename, dest_file),
            "new_qos": "hot",
            "new_filename": dest_file,
            "filename": filename,
        }

    def clear_hot(self, filename):
        print(f"Ready to delete {filename}")
        try:
            os.remove(re.sub("^file://[^/]*/", "", filename))
        except Exception as e:
            print(f"Error deleting the file {filename} :*(", e)

    def settings(self):
        """Return the settings of the storage."""
        return f"Storing hot in {self._hot_path}, cold in {self._cold_path} using {self._transfer} to transfer the files"
