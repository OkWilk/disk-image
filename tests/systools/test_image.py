import unittest
import time
import src.systools.image as image
from src.systools.runcommand import Execute


class ImageTest(unittest.TestCase):

    def test_fs_to_command_mapping(self):
        """This test require partclone to be present on the development box."""
        clone = image.PartitionImage('sdb', None)
        for command in clone._fs_to_command.values():
            runner = Execute([command, '-v']).run()
            self.assertEqual(runner, 0)

    def test_image_pendrive(self):
        clone = image.PartitionImage('sdb', '/tmp/',overwrite=True,refresh_delay=2,log=True)
        clone.backup()
