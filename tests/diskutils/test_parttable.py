import unittest
from unittest.mock import Mock, patch
import src.diskutils.diskdetect as diskdetect


class DiskDetectTest(unittest.TestCase):

    def setUp(self):
        self.parser = diskdetect._LsblkOutputParser()

    def test_detect_disks(self):
        result = diskdetect.detect_disks()
        self.assertTrue('sda' in str(result))
        self.assertTrue('sda1' in str(result))

    @patch('src.diskutils.diskdetect.logging')
    def test_extract_pairs_with_missing_name_raises_exception(self, log_mock):
        test_str = 'TYPE="disk" FSTYPE="test_val" SIZE="256060514304"'
        with self.assertRaises(ValueError):
            extracted = self.parser._extract_pairs_to_dict(test_str)
            self.assertEqual(1, log_mock.error.call_count)
