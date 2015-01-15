from urlparse import urlparse
import os
from cStringIO import StringIO
import gzip

__version__ = '0.3'


class UnsupportedProtocolError(Exception):
    pass


def _parse_s3_url(uri_string):
    parsed_url = urlparse(uri_string)
    if parsed_url.scheme != "s3":
        raise UnsupportedProtocolError("must be a s3 url")
    bucket = parsed_url.netloc
    filename = parsed_url.path.lstrip("/")
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
        from boto.s3.connection import S3Connection

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

        def handle_eof():
            raise StopIteration

        return self._next(handle_eof)

    def readline(self):

        def handle_eof():
            return ""

        return self._next(handle_eof)

    def _next(self, handle_eof):
        """
        Get the next line, and handles the EOF based the function handle_eof
        """
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
                        return handle_eof()
            else:
                if self._next_file():
                    return self.next()
                else:
                    return handle_eof()
        except IndexError:
            return handle_eof()

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
            if self._current_part_file:
                self._current_part_file.close()
            if key.name.endswith(".gz"):
                gzf = gzip.GzipFile(fileobj=f)
                self._current_part_file = gzf
            else:
                self._current_part_file = f
            return True
        except IndexError:
            return False

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        """
        Close the connection
        """
        if self._current_part_file:
            self._current_part_file.close()
