import unittest
from unittest.mock import Mock, patch
import src.systools.diskdetect as diskdetect


class DiskDetectTest(unittest.TestCase):

    def setUp(self):
        self.parser = diskdetect._DiskParser()

    def test_detect_disks(self):
        result = diskdetect.detect_disks()
        self.assertTrue('sda' in str(result))
        self.assertTrue('sda1' in str(result))

    def test_ignore_list_works(self):
        test_str = 'KNAME="sda" TYPE="loop" FSTYPE="test_val" \
                    SIZE="256060514304"'
        self.parser.parse(test_str)
        self.assertFalse('sda' in str(self.parser.output))
        self.assertEqual({}, self.parser.output)

    def test_extract_pairs_to_dict(self):
        test_str = 'KNAME="sda" TYPE="disk" FSTYPE="test_val" \
                    SIZE="256060514304"'
        extracted = self.parser._extract_pairs_to_dict(test_str)
        self.assert_extracted_values(extracted)

    def test_extract_pairs_with_unknown_key(self):
        test_str = 'KNAME="sda" UNKNOWN="unknown_value" TYPE="disk" \
                    FSTYPE="test_val" SIZE="256060514304"'
        extracted = self.parser._extract_pairs_to_dict(test_str)
        self.assert_extracted_values(extracted)
        self.assertFalse('UNKNOWN' in str(extracted))

    def test_extract_pairs_with_missing_key(self):
        test_str = 'KNAME="sda" FSTYPE="test_val" SIZE="256060514304"'
        extracted = self.parser._extract_pairs_to_dict(test_str)
        self.assertEqual(extracted['KNAME'], 'sda')
        self.assertEqual(extracted['FSTYPE'], 'test_val')
        self.assertEqual(extracted['SIZE'], '256060514304')
        self.assertFalse('TYPE' in extracted)

    @patch('src.systools.diskdetect.logging')
    def test_extract_pairs_with_missing_name_raises_exception(self, log_mock):
        test_str = 'TYPE="disk" FSTYPE="test_val" SIZE="256060514304"'
        with self.assertRaises(ValueError):
            extracted = self.parser._extract_pairs_to_dict(test_str)
            self.assertEqual(log_mock.error.call_count, 1)

    def assert_extracted_values(self, extracted, name='sda', type='disk',
                                fstype='test_val',size='256060514304'):
        self.assertEqual(extracted['KNAME'], 'sda')
        self.assertEqual(extracted['TYPE'], 'disk')
        self.assertEqual(extracted['FSTYPE'], 'test_val')
        self.assertEqual(extracted['SIZE'], '256060514304')
