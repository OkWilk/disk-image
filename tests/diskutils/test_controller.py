"""Author: Oktawiusz Wilk
Date: 30/11/2015
"""

import unittest
import time
from unittest.mock import Mock, patch
from src.diskutils.controller import ProcessController, BackupController, RestorationController

# TODO: add ProcessController tests.


class BackupControllerTest(unittest.TestCase):
    DRIVE = 'sda'
    JOB = 'test_job'
    DEFAULT_CONFIG = {
        'overwrite': False,
        'rescue': False,
        'space_check': True,
        'fs_check': True,
        'crc_check': True,
        'force': False,
        'refresh_delay': 5,
        'compress': False,
    }

    @patch('src.diskutils.controller.PartitionImage')
    @patch('src.diskutils.controller.DiskLayout')
    # @patch('src.diskutils.controller.detect_disks')
    def setUp(self, layout_mock, image_mock):
        disk_layout = Mock()
        disk_layout.detect_layout.return_value = 'MBR'
        layout_mock.with_config.return_value = disk_layout
        partition_image = Mock()
        partition_image.get_status.return_value = ''
        image_mock.with_config.return_value = partition_image
        self.controller = BackupController(self.DRIVE, self.JOB, self.DEFAULT_CONFIG)
        self.backup_path = self.controller.BACKUP_PATH + self.JOB + '/'

    # TODO: add test_backup_controller_creates_backupset

    def test_status_is_initialized(self):
        self.assert_status()

    @patch('src.diskutils.controller.makedirs')
    @patch('src.diskutils.controller.path')
    def test_create_backup_directory(self, path_mock, makedirs_mock):
        path_mock.exists.return_value = False
        self.controller._create_backup_directory()
        self.assertTrue(path_mock.exists.called)
        self.assertTrue(makedirs_mock.called)

    @patch('src.diskutils.controller.makedirs')
    @patch('src.diskutils.controller.path')
    def test_create_backup_directory_skip_if_exists(self, path_mock, makedirs_mock):
        path_mock.exists.return_value = True
        self.assertFalse(makedirs_mock.called)

    @patch('src.diskutils.controller.logging')
    @patch('src.diskutils.controller.makedirs')
    @patch('src.diskutils.controller.path')
    def test_create_backup_directory_raises_on_io_errors(self, path_mock,
                                                         makedirs_mock, logger_mock):
        path_mock.exists.return_value = False
        makedirs_mock.side_effect = IOError()
        with self.assertRaises(IOError):
            self.controller._create_backup_directory()
            self.assertTrue(makedirs_mock.called)
            logger_mock.assert_called_with('Cannot create path for backup: ' +
                          self.backup_path + '. Cause:' + 'error message')

    def test_backup(self):
        self.assertFalse(self.controller._thread)
        self.controller.backup()
        self.assertTrue(self.controller._thread)
        self.controller._thread.join()
        print(str(self.controller.get_status()))
        self.assert_status(status='finished', path=self.backup_path, layout='MBR',
                           start_time=True, end_time=True)

    def test_get_status_runs_update(self):
        self.controller._imager.get_status = Mock()
        self.controller._imager.get_status.return_value = 'updated_status'
        status = self.controller.get_status()
        self.assertEqual('updated_status', status['partitions'])

    def test_get_status_skip_update_if_no_imager_available(self):
        self.controller._imager = None
        status = self.controller.get_status()
        self.assertEqual('', status['partitions'])

    def test_sets_status_to_error_with_message(self):
        self.controller._imager = Mock()
        self.controller._imager.backup.side_effect = Exception("message")
        self.controller._backup()
        self.assertEqual('error', self.controller._status['status'])
        self.assertTrue(self.controller._status['error_msg'])

    def assert_status(self, status='', path='', layout='', partitions='',
                      start_time=False, end_time=False):
        self.assertEqual(status, self.controller._status['status'])
        self.assertEqual(path, self.controller._status['path'])
        self.assertEqual(layout, self.controller._status['layout'])
        self.assertEqual(partitions, self.controller._status['partitions'])
        self.assertEqual(start_time, bool(self.controller._status['start_time'].strip()))
        self.assertEqual(end_time, bool(self.controller._status['end_time'].strip()))
        if start_time and end_time:
            start_time_value = self._time_to_value(self.controller._status['start_time'])
            end_time_value = self._time_to_value(self.controller._status['end_time'])
            self.assertTrue(start_time_value <= end_time_value)

    def _time_to_value(self, time_string:str):
        return time.strptime(time_string, '%d/%m/%Y %H:%M:%S')

        # TODO: add RestorationController tests.
