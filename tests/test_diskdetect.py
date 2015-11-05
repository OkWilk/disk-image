import unittest
import src.systools.diskdetect as diskdetect


class DiskDetectTest(unittest.TestCase):

    def test_detect_disks(self):
        result = diskdetect.detect_disks()
        self.assertTrue('sda' in result)
        self.assertFalse('sda1' in result)
        "test".encode("utf-8")
