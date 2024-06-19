import os
import gfal2

# This requires to install swig: yum install swig. Let's wait a bit
import fts3.rest.client.easy as fts3


class TransferManager:
    def __init__(self):
        endpoint = os.environ["INVENIO_FTS_ENDPOINT"]
        self._context = fts3.Context(endpoint, verify=True)

    def _submit(self, job):
        #print("Submiting to fts", job)
        try:
            job_id = fts3.submit(self._context, job)
        except Exception as my_exc:
            print("Error submitting to fts", my_exc)
            return None
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
            "copy_pin_lifetime": -1,
        }
        # internal retry logic in case of fail and overwrite to true if it has failed
        a= self._submit(job)
        #print("FTS RETURNS",a)
        return a

    def transfer_status(self, transfer_id):
        try:
            fts_status = fts3.get_job_status(self._context, transfer_id)
        except:
            print("Error connecting to fts")
            return None, None
        if not fts_status:
            print("Error retrieving the status from fts")
            return None, None
        if 'job_state' not in fts_status:
            print("The response does not have 'job_state'")
            return None, None
        # print("The status in fts is", fts_status['job_state'])
        if fts_status['job_state'] == 'FINISHED':
            return "DONE", None
        return fts_status['job_state'], fts_status['reason']


    def get_endpoint_info(self):
        return self._context.get_endpoint_info()

    def whoami(self):
        return fts3.whoami(self._context)

    def exists_file(self, filename):
        ctx = gfal2.creat_context()

        print(f"Checking with gfal if {filename} exists")
        try:
            info = ctx.stat(filename)
            checksum = ctx.checksum(filename, "ADLER32")

            return {"size": info.st_size, "checksum": checksum}
        except:
            pass
        return False
