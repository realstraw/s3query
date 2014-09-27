from urlparse import urlparse
from boto.s3.connection import S3Connection
import os
from cStringIO import StringIO


def _parse_s3_url(uri_string):
    parsed_url = urlparse(uri_string)
    if parsed_url.scheme != "s3":
        return False
    bucket = parsed_url.netloc
    filename = parsed_url.path
    filename = filename[1:] if filename[0] == "/" else filename
    return bucket, filename


def s3open(s3url, aws_access_key_id=None, aws_secret_access_key=None):
    bucket, filename = _parse_s3_url(s3url)
    if not aws_access_key_id:
        aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    if not aws_secret_access_key:
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    return S3File(bucket, filename, aws_access_key_id, aws_secret_access_key)


class S3File(object):

    def __init__(
            self, bucket, filename, aws_access_key_id, aws_secret_access_key):
        self._conn = S3Connection(aws_access_key_id, aws_secret_access_key)
        self.bucket = self._conn.get_bucket(bucket)
        if filename.endswith("/"):
            self.partfiles = [key for key in self.bucket.list(prefix=filename)]
        else:
            self.partfiles = [self.bucket.get_key(filename), ]

        self._current_part_file_id = 0
        self._current_part_file = None

    def __iter__(self):
        return self

    def next(self):
        try:
            if self._current_part_file:
                # if already have an open file
                line = self._current_part_file.readline()
                if line:
                    return line
                else:  # reach the end of line of the current file
                    if self._next_file():
                        return self.next()
                    else:
                        raise StopIteration
            else:
                if self._next_file():
                    return self.next()
                else:
                    raise StopIteration
        except IndexError:
            raise StopIteration

    def _next_file(self):
        """
        Return true if there is next file, false otherwise
        """
        try:
            key = self.partfiles[self._current_part_file_id]
            self._current_part_file_id += 1
            f = StringIO()
            key.get_contents_to_file(f)
            f.seek(0)
            self._current_part_file = f
            return True
        except IndexError:
            return False

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def close(self):
        """
        Close the connection
        """
        # TODO implement
        raise NotImplementedError
