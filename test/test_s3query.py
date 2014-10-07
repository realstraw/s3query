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

    def _test_iterate(self, s3_loc, output_lines):
        ref_lines = iter(output_lines)
        with s3open(s3_loc) as s3file:
            for line in s3file:
                self.assertEqual(line, ref_lines.next())

        # Assert there is no lines in output_lines by check ref_lines throws
        # StopIteration exception
        self.assertRaises(StopIteration, ref_lines.next)

    def _test_readline(self, s3_loc, output_lines):
        ref_lines = iter(output_lines)
        with s3open(s3_loc) as s3file:
            line = s3file.readline()
            while line != "":
                self.assertEqual(line, ref_lines.next())
                line = s3file.readline()

            self.assertEqual("", s3file.readline())

        # Assert there is no lines in output_lines by check ref_lines throws
        # StopIteration exception
        self.assertRaises(StopIteration, ref_lines.next)

    def test_iterate_plain_files(self):
        """
        Test it iterates the plain files on S3, given a directory
        """
        s3_loc = "s3://s3query/data/test/"
        output_lines = [
            "one\n", "two\n", "three\n", "four\n", "five\n", "six\n",
            "seven\n", "eight\n", "nine\n"]
        self._test_iterate(s3_loc, output_lines)

    def test_readline_plain_files(self):
        """
        Test the readline function on plain files
        """
        s3_loc = "s3://s3query/data/test/"
        output_lines = [
            "one\n", "two\n", "three\n", "four\n", "five\n", "six\n",
            "seven\n", "eight\n", "nine\n"]
        self._test_readline(s3_loc, output_lines)

    def test_read_single_plain_file(self):
        """
        Test reading single plain file
        """
        s3_loc = "s3://s3query/data/test/part_2.txt"
        output_lines = ["four\n", "five\n", "six\n", "seven\n"]
        self._test_iterate(s3_loc, output_lines)
        self._test_readline(s3_loc, output_lines)

    def test_iterate_gz_files(self):
        s3_loc = "s3://s3query/data/testgz/"
        output_lines = [
            "one\n", "two\n", "three\n", "four\n", "five\n", "six\n",
            "seven\n", "eight\n", "nine\n"]
        self._test_iterate(s3_loc, output_lines)

    def test_readline_gz_files(self):
        s3_loc = "s3://s3query/data/testgz/"
        output_lines = [
            "one\n", "two\n", "three\n", "four\n", "five\n", "six\n",
            "seven\n", "eight\n", "nine\n"]
        self._test_readline(s3_loc, output_lines)

    def test_read_single_gz_file(self):
        s3_loc = "s3://s3query/data/testgz/part_2.txt.gz"
        output_lines = ["four\n", "five\n", "six\n", "seven\n"]
        self._test_iterate(s3_loc, output_lines)
        self._test_readline(s3_loc, output_lines)


if __name__ == '__main__':
    unittest.main()
