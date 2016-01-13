"""
Author: Oktawiusz Wilk
Date: 30/11/2015
"""

from unittest import TestCase
from unittest.mock import Mock, patch
import src.constants as constants
from src.core.controller import ProcessController, BackupController#, RestorationController


class ProcessControllerTest(TestCase):

    def test_is_abstract_class(self):
        with self.assertRaises(Exception):
            controller = ProcessController()


class BackupControllerTest(TestCase):

    @patch('src.core.controller.PartitionImage')
    @patch('src.core.controller.DiskLayout')
    @patch('src.core.controller.MongoDB')
    @patch('src.core.controller.ConfigHelper')
    def setUp(self, config_mock, db_class, disk_layout_class, partition_image_class):
        self.config_mock = config_mock
        db_class.return_value = self.db_mock = Mock()
        self.disk_layout_mock = Mock()
        disk_layout_class.return_value = self.disk_layout_mock
        disk_layout_class.with_config.return_value = self.disk_layout_mock
        partition_image_class.return_value = self.partition_image_mock = Mock()
        self.disk = 'sda'
        self.job = 'test_job'
        self.config = {
            'overwrite': False,
            'rescue': False,
            'space_check': True,
            'fs_check': True,
            'crc_check': True,
            'force': False,
            'refresh_delay': 5,
            'compress': False,
        }
        self.backup_controller = BackupController(self.disk, self.job, self.config)

    @patch('src.core.controller.makedirs')
    @patch('src.core.controller.path')
    def test_create_backup_directory(self, path_mock, makedirs_mock):
        path_mock.exists.return_value = False
        self.backup_controller._create_backup_directory()
        self.assertTrue(path_mock.exists.called)
        self.assertTrue(makedirs_mock.called)

    @patch('src.core.controller.makedirs')
    @patch('src.core.controller.path')
    def test_create_backup_directory_skip_if_exists(self, path_mock, makedirs_mock):
        path_mock.exists.return_value = True
        self.backup_controller._create_backup_directory()
        self.assertFalse(makedirs_mock.called)

    @patch('src.core.controller.logging')
    @patch('src.core.controller.makedirs')
    @patch('src.core.controller.path')
    def test_create_backup_directory_raises_on_io_errors(self, path_mock, makedirs_mock, logger_mock):
        path_mock.exists.return_value = False
        makedirs_mock.side_effect = IOError()
        with self.assertRaises(IOError):
            self.backup_controller._create_backup_directory()
            self.assertTrue(makedirs_mock.called)
            logger_mock.assert_called_with('Cannot create path for backup: ' +
                          self.backup_path + '. Cause:' + 'error message')

    @patch('src.core.controller.DiskLayout')
    @patch('src.core.controller.path')
    def test_backup(self, path_mock, disk_layout_mock):
        path_mock.exists.return_value = True
        disk_layout_mock.detect_layout.return_value = 'MBR'
        self.assertFalse(self.backup_controller._thread)
        self.backup_controller._complete_backupset = Mock()
        self.backup_controller.run()
        self.assertTrue(self.backup_controller._thread)
        self.backup_controller._thread.join()
        # print(str(self.backup_controller.get_status()))
        # assert_status(self.backup_controller, operation='Backup', status='finished', layout='MBR')
# #
#     def test_get_status_runs_update(self):
#         self.backup_controller._imager.get_status = Mock()
#         self.controller._imager.get_status.return_value = 'updated_status'
#         status = self.controller.get_status()
#         self.assertEqual('updated_status', status['partitions'])
#
#     def test_get_status_skip_update_if_no_imager_available(self):
#         self.controller._imager = None
#         status = self.controller.get_status()
#         self.assertEqual('', status['partitions'])
#
    @patch('src.core.controller.path')
    def test_sets_status_to_error_with_message(self, path_mock):
        self.backup_controller._complete_backupset = Mock()
        path_mock.exists.return_value = True
        self.backup_controller._imager = Mock()
        self.backup_controller._imager.backup.side_effect = Exception("message")
        self.backup_controller._backup()
        self.assertEqual('error', self.backup_controller._status['status'])
        self.assertTrue(self.backup_controller._status['error_msg'])
#

#


    # TODO: add RestorationController tests.


def assert_status(controller, operation='', status='', layout='', partitions=''):
    assert(status == controller.get_status()['status'])
    assert(layout == controller._status['layout'])
    assert(partitions == controller._status['partitions'])
    assert(start_time == bool(controller._status['start_time'].strip()))
    assert(end_time == bool(controller._status['end_time'].strip()))


def _time_to_value(time_string:str):
    return time.strptime(time_string, '%d/%m/%Y %H:%M:%S')
