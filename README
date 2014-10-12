# S3Query

S3Query opens file or files in s3 file system as text file, will perform unzip
if necessary. It also handles hadoop liked part-\* files and treat all files
within a directory as one single file.


# Example

    >>> from s3query import s3open
    >>> import sys
    >>>
    >>> with s3open("s3://s3query/data/testgz/") as fgz:
    ...     for line in fgz:
    ...         sys.stdout.write(line)
    ...
    one
    two
    three
    four
    five
    six
    seven
    eight
    nine
    >>> with s3open("s3://s3query/data/test/") as fplain:
    ...     for line in fplain:
    ...         sys.stdout.write(line)
    ...
    one
    two
    three
    four
    five
    six
    seven
    eight
    nine
