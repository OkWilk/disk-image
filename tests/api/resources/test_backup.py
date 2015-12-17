

import unittest
from unittest.mock import Mock, patch
from src.api.resources.backup import Backup


class BackupTest(unittest.TestCase):

    def test_start(self):
        backup = Backup()
        self.assertTrue(backup)
