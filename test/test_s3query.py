import unittest
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from s3query import s3open


class TestS3Query(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_iterate_plain_files(self):
        """
        Test it iterates the plain files on S3, given a directory
        """
        s3_loc = "s3://s3query/data/test/"
        output_lines = [
            "one\n", "two\n", "three\n", "four\n", "five\n", "six\n",
            "seven\n", "eight\n", "nine\n"]
        ref_lines = iter(output_lines)
        with s3open(s3_loc) as s3file:
            for line in s3file:
                self.assertEqual(line, ref_lines.next())

        # Assert there is no lines in output_lines by check ref_lines throws
        # StopIteration exception
        self.assertRaises(StopIteration, ref_lines.next)

        s3_loc = "s3://s3query/data/testgz/"
        ref_lines = iter(output_lines)

        with s3open(s3_loc) as s3file:
            for line in s3file:
                self.assertEqual(line, ref_lines.next())

        # Assert there is no lines in output_lines by check ref_lines throws
        # StopIteration exception
        self.assertRaises(StopIteration, ref_lines.next)


if __name__ == '__main__':
    unittest.main()
