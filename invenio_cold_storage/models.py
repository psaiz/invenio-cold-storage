from invenio_db import db
from sqlalchemy_utils.types import UUIDType

from invenio_files_rest.models import Bucket, FileInstance, ObjectVersion

import uuid
from invenio_records.models import RecordMetadata
class FileIndex(db.Model):
    """Model for storing buckets.

    A bucket is a container of objects. Buckets have a default location and
    storage class. Individual objects in the bucket can however have different
    locations and storage classes.

    A bucket can be marked as deleted. A bucket can also be marked as locked
    to prevent operations on the bucket.

    Each bucket can also define a quota. The size of a bucket is the size
    of all objects in the bucket (including all versions).
    """

    __tablename__ = "cold_file_indexes"

    name = db.Column(
        UUIDType,
        primary_key=True,
        default=uuid.uuid4,
    )
    """Identifier of file."""

    storage_class = db.Column(db.String(1), nullable=True)
    """Storage class of file."""


    index_file_name = db.Column(db.String(255))
    """Name of the index file"""

    number_files = db.Column(db.BigInteger, nullable=True)
    """String representing the checksum of the object."""

    size = db.Column(db.BigInteger, default=0, nullable=True)
    """Size of all the files."""

    bucket_id = db.Column(
        UUIDType,
        db.ForeignKey(Bucket.id, ondelete="RESTRICT"),
        default=uuid.uuid4,
        nullable=False,
    )
    """Bucket identifier."""

    record_id = db.Column(
        UUIDType,
        db.ForeignKey(RecordMetadata.id),
        primary_key=True,
        nullable=False,
        # NOTE no unique constrain for better future ...
    )
    """Record related with the bucket."""


    bucket = db.relationship(Bucket)
    """Relationship to the bucket."""

    record = db.relationship(RecordMetadata)
    """It is used by SQLAlchemy for optimistic concurrency control."""

    _index_content = ""
    @classmethod
    def create(cls, record, index_file_name, index_content):
        """Create a new Dataset and adds it to the session.

        :param record: Record used to relate with the ``Bucket``.
        :param bucket: Bucket used to relate with the ``Record``.
        :returns: The :class:`~invenio_cold_storage.models.Dataset`
            object created.
        """
        bucket = Bucket.create()
        rb = cls(record=record, bucket=bucket)
        db.session.add(rb)
        rb.size = 0
        rb.number_files = 0
        rb.index_file_name = index_file_name
        rb._index_content = index_content
        for entry in index_content:
            entry_file = FileInstance.create()
            entry_file.set_uri(entry["uri"], entry["size"], entry["checksum"])
            ObjectVersion.create(bucket, f"{index_file_name}_{rb.number_files}", _file_id=entry_file.id)
            entry["file_id"]= str(entry_file.id)
            rb.number_files +=1
            rb.size += entry["size"]
        return rb
    @classmethod
    def delete_by_record(cls, record):
        buckets = []
        for index in cls.query.filter_by(record=record.model):
            if index.bucket not in buckets:
                buckets.append(index.bucket)
        cls.query.filter_by(record=record.model).delete()
        for bucket in buckets:
            for o in ObjectVersion.get_by_bucket(bucket).all():
                o.remove()
                FileInstance.query.filter_by(id=o.file_id).delete()
            bucket.remove()

    @classmethod
    def query_by_record(cls, record):
        return cls.query.filter_by(record=record.model).all()


    def print(self):
        return {'key': self.index_file_name,
                'number_files': self.number_files,
                'size': self.size,
                'files': self._index_content,
                'uri': 'hello_world'}
