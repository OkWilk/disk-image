from unittest import TestCase
from unittest.mock import Mock, patch
from src.core.diskdetect import DiskDetect, _LsblkOutputParser


class DiskDetectTest(TestCase):

    def setUp(self):
        self.diskdetect = DiskDetect
        # self.parser = diskdetect._LsblkOutputParser()

    def test_detect_disks(self):
        result = self.diskdetect.get_disk_list()
        self.assertTrue('sda' in str(result))
        self.assertTrue('sda1' in str(result))
        self.assertFalse('sr0' in str(result))

    def test_get_disk_details(self):
        result = self.diskdetect.get_disk_details('sda')
        self.assertTrue(result)

    def test_get_disk_details_raises_on_invalid_disk(self):
        with self.assertRaises(ValueError):
            self.diskdetect.get_disk_details('xxx')


class LsblkParserTest(TestCase):

    def setUp(self):
        self.parser = _LsblkOutputParser()

    def test_ignore_list_works(self):
        test_str = 'KNAME="sda" TYPE="loop" FSTYPE="test_val" \
                    SIZE="256060514304"'
        self.parser.parse(test_str)
        self.assertFalse('sda' in str(self.parser.output))
        self.assertEqual([], self.parser.output)

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
        self.assertEqual('sda', extracted['KNAME'])
        self.assertEqual('test_val', extracted['FSTYPE'])
        self.assertEqual('256060514304', extracted['SIZE'])
        self.assertFalse('TYPE' in extracted)

    @patch('src.core.diskdetect.logging')
    def test_extract_pairs_with_missing_name_raises_exception(self, log_mock):
        test_str = 'TYPE="disk" FSTYPE="test_val" SIZE="256060514304"'
        with self.assertRaises(ValueError):
            extracted = self.parser._extract_pairs_to_dict(test_str)
            self.assertEqual(1, log_mock.error.call_count)

    def assert_extracted_values(self, extracted, name='sda', type='disk',
                                fstype='test_val',size='256060514304'):
        self.assertEqual('sda', extracted['KNAME'])
        self.assertEqual('disk', extracted['TYPE'])
        self.assertEqual('test_val', extracted['FSTYPE'])
        self.assertEqual('256060514304', extracted['SIZE'])
