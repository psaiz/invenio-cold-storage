
from invenio_records_files.api import FileObject, Record, FilesIterator
from invenio_files_rest.models import Bucket, FileInstance, ObjectVersion, BucketTag, ObjectVersionTag
from invenio_records_files.models import RecordsBuckets
from collections import OrderedDict

import json
class FileObjectCold(FileObject):
    """Overwrite the fileobject to get multiple URI"""
    def dumps(self):
        """This one has the information about the cold URI stored in a ObjectVersionTag"""
        info = super(FileObjectCold, self).dumps()
        info["tags"] = {}
        for tagName in ("uri_cold", "hot_deleted"):
            tag = ObjectVersionTag.get(str(self.obj.version_id), tagName)
            if tag:
                info["tags"][tagName] = tag.value
        if "uri" not in info:
            file=FileInstance.get(str(self.obj.file_id))
            info["uri"]=file.uri
        return info

#    def loads(self):
#        """CHECKING"""
# #       print("LOADING A FILE WITH POSSIBLE COLD")
#        info = super(FileObjectCold, self).loads()
#        print(info)
#        return info
