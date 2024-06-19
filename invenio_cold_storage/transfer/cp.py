import shutil
from os import getpid, makedirs, path


class TransferManager:
    """Example of a TransferManager class, that defines the methods that other implementations should offer.
    This one uses `cp` to move the files around. Note that there are some simplifactions that could have been done
    for this particular class that are"""

    def __init__(self):
        self._pid = getpid()
        self._last_id = 0

    def _copy(self, source, dest):
        # print(f"Copying the file {source} into {dest}")
        makedirs(path.dirname(dest.replace("file://", "")), exist_ok=True)
        shutil.copyfile(source.replace("file://", ""), dest.replace("file://", ""))
        self._last_id += 1
        return f"{self._pid}_{self._last_id}"

    def stage(self, source, dest):
        """Asnychronous bring back a file from a `cold` storage, copying the {source} into the {dest}. It should return an id that can be queried to check the status

        Note that for `cp`, there is no real need to do it asynchrounously, and, indeed, the method does it on the spot.
        From the design point of view it is kept asynch to make it easier for other implementations
        """
        return self._copy(source, dest)

    def archive(self, source, dest):
        """Asnychronous store a file on a `cold` storage, copying the {source} into the {dest}. It should return an id that can be queried to check the status

        Note that for `cp`, there is no real need to do it asynchrounously, and, indeed, the method does it on the spot.
        From the design point of view it is kept asynch to make it easier for other implementations
        """
        return self._copy(source, dest)

    def transfer_status(self, _):
        """Return the status of a particular transfer"""
        return "DONE", None
