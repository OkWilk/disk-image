import unittest
from unittest.mock import Mock, patch
from src.diskutils.parttable import DiskLayout


class DiskDetectTest(unittest.TestCase):
    DRIVE = 'drive'
    PATH = 'path'

    def setUp(self):
        self.disk = DiskLayout(self.DRIVE, self.PATH)

    @patch('src.diskutils.parttable.logging')
    def test_disk_layout_with_config_raises_on_missing_key(self, logger_mock):
        config = {'random': False}
        with self.assertRaises(Exception):
            disk = DiskLayout.with_config('sda', '/tmp', config)
        self.assertTrue(logger_mock.error.called)

    def test_disk_layout_with_config(self):
        config = {'overwrite': True}
        disk = DiskLayout.with_config('sda', '/tmp', config)
        self.assertTrue(disk.overwrite)

    @patch('src.diskutils.parttable.Execute')
    def test_detect_layout(self, execute_mock):
        execute = Mock()
        execute_mock.return_value = execute
        execute.output.return_value = 'Partition Table: msdos'
        self.assertEqual('MBR', self.disk.detect_layout())
        execute.output.return_value = 'Partition Table: gpt'
        self.assertEqual('GPT', self.disk.detect_layout())
        execute.output.return_value = 'Partition Table: loop'
        self.assertEqual('UNKNOWN', self.disk.detect_layout())

    def test_backup_layout_mbr(self):
        self.disk.detect_layout = Mock()
        self.disk.detect_layout.return_value = 'MBR'
        self.disk._backup_mbr = Mock()
        self.disk._backup_mbr_partition_table = Mock()
        self.disk.backup_layout()
        self.disk._backup_mbr.assert_called()
        self.disk._backup_mbr_partition_table.assert_called()

    def test_backup_layout_gpt(self):
        self.disk.detect_layout = Mock()
        self.disk.detect_layout.return_value = 'GPT'
        self.disk._backup_gpt_layout = Mock()
        self.disk.backup_layout()
        self.disk._backup_gpt_layout.assert_called()

    def test_backup_unknown_layout_raises_exception(self):
        self.disk.detect_layout = Mock()
        self.disk.detect_layout.return_value = 'UNKNOWN'
        with self.assertRaises(Exception):
            self.disk.backup_layout()

    def test_backup_gpt_layout(self):
        with self.assertRaises(NotImplementedError):
            self.disk._backup_gpt_layout()

    def test_restore_gpt_layout(self):
        with self.assertRaises(NotImplementedError):
            self.disk._restore_gpt_layout()

    # TODO: write new partition table restoration test
    # def test_restore_mbr_layout(self):
    #     with self.assertRaises(NotImplementedError):
    #         self.disk._restore_mbr_layout()

    @patch('src.diskutils.parttable.path')
    @patch('src.diskutils.parttable.Execute')
    def test_backup_mbr(self, exec_mock, path_mock):
        self.disk = DiskLayout(self.DRIVE, self.PATH, overwrite=True)
        runner = Mock()
        exec_mock.return_value = runner
        runner.run.return_value = 0
        path_mock.exists.return_value = True
        self.disk._backup_mbr()
        self.assertTrue(runner.run.called)
        self.assertTrue(path_mock.exists.called)

    @patch('src.diskutils.parttable.path')
    def test_backup_mbr_does_not_overwrite(self, path_mock):
        path_mock.exists.return_value = True
        with self.assertRaises(Exception):
            self.disk._backup_mbr()

    @patch('src.diskutils.parttable.Execute')
    @patch('src.diskutils.parttable.logging')
    def test_backup_mbr_raises_if_no_file_created(self, logger_mock, exec_mock):
        with self.assertRaises(Exception):
            self.disk._backup_mbr()
        logger_mock.error.assert_called_with('MBR backup failed, disk:drive, target:path')

    @patch('src.diskutils.parttable.path')
    @patch('src.diskutils.parttable.Execute')
    def test_backup_mbr_partition_table(self, exec_mock, path_mock):
        self.disk = DiskLayout(self.DRIVE, self.PATH, overwrite=True)
        runner = Mock()
        exec_mock.return_value = runner
        runner.run.return_value = 0
        path_mock.exists.return_value = True
        self.disk._backup_mbr_partition_table()
        self.assertTrue(path_mock.exists.called)
        self.assertTrue(runner.run.called)

    @patch('src.diskutils.parttable.logging')
    @patch('src.diskutils.parttable.path')
    def test_backup_mbr_does_not_overwrite(self, path_mock, logger_mock):
        path_mock.exists.return_value = True
        with self.assertRaises(Exception):
            self.disk._backup_mbr_partition_table()
        logger_mock.error.assert_called_with('Existing backup detected at pathptable.bak. Not overwritting.')

    @patch('src.diskutils.parttable.logging')
    @patch('src.diskutils.parttable.Execute')
    def test_backup_mbr_partition_table_raises_if_no_file_created(self, exec_mock, logger_mock):
        with self.assertRaises(Exception):
            self.disk._backup_mbr_partition_table()
        logger_mock.error.assert_called_with('Partition table backup failed, disk:drive, target:path')
