

import unittest
from unittest.mock import Mock, patch
from src.api.resources.job import Job


class BackupTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_start(self):
        job = Job()
        self.assertTrue(job)
