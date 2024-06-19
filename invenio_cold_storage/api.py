
from invenio_records_files.api import Record as _Record
class Record(_Record):
    def __init__(self, *args, **kwargs):
        """Initialize the record."""
        print("INITIALIZING A Record WITH FILE INDEXES")
        self._file_indexes = None
        super(Record, self).__init__(*args, **kwargs)


    @classmethod
    def create(cls, data, id_=None, with_bucket=True, **kwargs):
        """Create a record and the associated bucket.

        :param with_bucket: Create a bucket automatically on record creation.
        """
        # Create bucket and store in record metadata.
        print("CREATING WITH FILE INDEXES")
        # Create the record
        record = super(Record, cls).create(data, id_=id_,with_bucket=with_bucket, **kwargs)
        return record

##    @property
 #   def file_indexes(self):
 #       """Get files iterator.#
#
#        :returns: Files iterator.
#        """
#        print("INT HTE GEETTER OF THE FILE_INDEXES")
#        if self.model is None:
#           raise MissingModelError()
#
#        file_indexes =FileIndex.query.filter_by(
#            record_id=self.id
#        ).first()#
#
#        if not records_buckets:
#            return None
#        else:
#            bucket = records_buckets.bucket
#
#        return self.files_iter_cls(self, bucket=bucket, file_cls=self.file_cls)

#    @file_indexes.setter
#    def file_indexes(self, data):
#        """Set file_indexes from data."""
#        print("IBN THE SETTER OF THE FILE_INDEX")
#        current_indices = self.file_indexes
#        if self._file_indexes:
#            raise RuntimeError("Can not update existing file indices.")
#        for key in data:
#            current_indices[key] = data[key]

#    def flush(self):
#        """Flush changes."""
#        print("LET'S FLUSH EVERYTHING")
#        self.files.flush()