import os

# This requires to install swig: yum install swig. Let's wait a bit
# import fts3.rest.client.easy as fts3


class TransferManager:
    def __init__(self):
        endpoint = os.environ["INVENIO_FTS_ENDPOINT"]
        self._context = fts3.Context(endpoint, verify=True)

    def _submit(self, job):
        job_id = fts3.submit(self._context, job)
        return job_id

    def _basic_job(self, source, dest):
        return {"files": [{"sources": [source], "destinations": [dest]}]}

    def stage(self, source, dest):
        job = self._basic_job(source, dest)

        job["params"] = {"bring_online": 604800, "copy_pin_lifetime": 64000}
        return self._submit(job)

    def archive(self, source, dest):
        job = self._basic_job(source, dest)
        job["params"] = {
            "archive_timeout": 86400,
            "copy_pin_lifetime": 64000,
        }
        # internal retry logic in case of fail and overwrite to true if it has failed

        return self._submit(job)

    def transfer_status(self, transfer_id):
        return fts3.get_job_status(self._context, transfer_id)

    def get_endpoint_info(self):
        return self._context.get_endpoint_info()

    def whoami(self):
        return fts3.whoami(self._context)
