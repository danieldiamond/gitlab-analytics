import os.path
import logging

from shared_modules.elt.error import ExtractError
from google.cloud.storage import client
from google.cloud.storage.bucket import Bucket


def list_gcs_directories(bucket):
    # from https://github.com/GoogleCloudPlatform/google-cloud-python/issues/920
    iterator = bucket.list_blobs(delimiter='/')
    prefixes = set()
    for page in iterator.pages:
        prefixes.update(page.prefixes)

    return prefixes

class Fetcher:
    def __init__(self, project=None, bucket=None, output_path=None):
        self.client = client.Client(project=project)
        # HACK: using self.client.get_bucket(bucket) raises 403
        self.bucket = Bucket(self.client, name=bucket)
        self._prefix = None
        self.output_path = output_path or os.path.join("/tmp", "gitlab-elt")

    @property
    def prefix(self):
        if not self._prefix:
            self._prefix = self.latest_prefix()

        return self._prefix

    def latest_prefix(self):
        prefixes = sorted(
            list_gcs_directories(self.bucket),
            reverse=True
        )

        if prefixes:
            return prefixes[0]

        raise ExtractError("Fetcher: bucket {} is empty.".format(self.bucket.name))


    def fetch_schema(self):
        return self.fetch_file("schema.yml")


    def fetch_files(self):
        return filter(lambda blob: blob.name.endswith(".csv.gz"),
                      self.bucket.list_blobs(prefix=self.prefix))


    def fetch_file(self, filename):
        blob = self.bucket.get_blob(os.path.join(self.prefix, filename))
        output_file = self.blob_file_path(blob)
        blob.download_to_filename(output_file)

        return output_file


    def download(self, blob):
        file_path = self.blob_file_path(blob)
        logging.info("downloading {} to {}".format(blob.name, file_path))
        blob.download_to_filename(file_path)

        return file_path


    def blob_file_path(self, blob):
        file_path = os.path.join(self.output_path, blob.name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        return file_path
