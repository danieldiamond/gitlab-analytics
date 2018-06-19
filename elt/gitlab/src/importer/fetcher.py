import os.path

from elt.error import ExtractError
from google.cloud.storage import client


def list_gcs_directories(bucket):
    # from https://github.com/GoogleCloudPlatform/google-cloud-python/issues/920
    iterator = bucket.list_blobs(delimiter='/')
    prefixes = set()
    for page in iterator.pages:
        prefixes.update(page.prefixes)

    return prefixes

class Fetcher:
    def __init__(self):
        self.client = client.Client(project="gitlab-analysis")
        self._bucket = None

    @property
    def bucket(self):
        if self._bucket is None:
            self._bucket = self.client.get_bucket("gitlab-elt")

        return self._bucket

    def latest_prefix(self):
        prefixes = sorted(
            list_gcs_directories(self.bucket),
            reverse=True
        )

        if prefixes:
            return prefixes[0]

        raise ExtractError("Fetcher: bucket {} is empty.".format(self.bucket.name))

    def fetch_schema(self, prefix):
        return fetch_file(prefix, filename)

    def fetch_file(self, prefix, filename):
        blob = self.bucket.get_blob(os.path.join(prefix, filename))
        blob.download_to_filename(filename)

        return filename
