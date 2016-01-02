import unittest
from unittest.mock import Mock, patch
from src.backupset import BackupSet
import src.diskutils.image as image



class ImageTest(unittest.TestCase):
    BACKUPSET_MOCK_VALUES = {
        'compressed': False,
        'partitions': [{'partition': '1', 'fs': 'vfat', 'size': '4051668992'}],
        'boot_record': '/tmp/sdxx/mbr.img',
        'creation_date': '28/12/2015 21:28:15',
        'id': 'sdxx',
        'disk_size': '4051697664',
        'backup_size': 540075337,
        'partition_table': '/tmp/sdxx/ptable.bak',
        'disk_layout': 'MBR'
    }
    BACKUPSET = BackupSet.from_json(BACKUPSET_MOCK_VALUES)

    def setUp(self):
        self.clone = image.PartitionImage('sdxx', '/tmp/', self.BACKUPSET)
        self.source = '/source/file'
        self.target = '/target/file'
        self.fs = 'ntfs'

    def test_core_fs_are_supported(self):
        self.assertEqual('partclone.ntfs', self.clone._fs_to_command['ntfs'])
        self.assertEqual('partclone.vfat', self.clone._fs_to_command['vfat'])
        self.assertEqual('partclone.ext3', self.clone._fs_to_command['ext3'])
        self.assertEqual('partclone.ext4', self.clone._fs_to_command['ext4'])
        self.assertEqual('partclone.hfsp', self.clone._fs_to_command['hfsplus'])
        self.assertEqual('partclone.hfsp', self.clone._fs_to_command['hfs+'])
        self.assertEqual('partclone.hfsp', self.clone._fs_to_command['hfs'])
        self.assertEqual('partclone.dd', self.clone._fs_to_command['raw'])

    def test_select_command_for_valid_fs(self):
        self.assertEqual(self.clone._fs_to_command['ntfs'],
                         self.clone._select_command_by_fs('ntfs'))

    def test_select_command_for_unknown_fs(self):
        self.assertEqual(self.clone._fs_to_command['raw'],
                         self.clone._select_command_by_fs(''))

    def test_select_command_for_invalid_fs(self):
        self.assertEqual(self.clone._fs_to_command['raw'],
                         self.clone._select_command_by_fs('invalid'))

    def test_init_status(self):
        self.assertTrue('sdxx1' in self.clone._status)
        self.assertEqual(self.clone.STATUS_PENDING, self.clone._status['sdxx1']['status'])

    def test_get_status(self):
        self.assertTrue('sdxx1' in self.clone.get_status())

    def test_image_created_with_config(self):
        config = {
            'overwrite': True, 'rescue': True, 'space_check': True,
            'fs_check': True, 'crc_check': True, 'force': True,
            'refresh_delay': 10, 'compress': True
        }
        imager = image.PartitionImage.with_config('sdxx', '/tmp', self.BACKUPSET, config)
        self.assert_config(imager, overwrite=config['overwrite'],
                           rescue=config['rescue'], space_check=config['space_check'],
                           fs_check=config['fs_check'], crc_check=config['crc_check'],
                           force=config['force'], refresh_delay=config['refresh_delay'],
                           compress=config['compress'])

    def test_image_created_with_config2(self):
        config = {
            'overwrite': False, 'rescue': False, 'space_check': False,
            'fs_check': False, 'crc_check': False, 'force': False,
            'refresh_delay': 7, 'compress': False, 'random_key': 231312
        }
        imager = image.PartitionImage.with_config('sdxx', '/tmp', self.BACKUPSET, config)
        self.assert_config(imager, overwrite=config['overwrite'],
                           rescue=config['rescue'], space_check=config['space_check'],
                           fs_check=config['fs_check'], crc_check=config['crc_check'],
                           force=config['force'], refresh_delay=config['refresh_delay'],
                           compress=config['compress'])

    @patch('src.diskutils.image.logging')
    def test_image_with_config_raises_on_missing_key(self, logger_mock):
        config = {'overwrite': True, 'random': False}
        with self.assertRaises(Exception):
            imager = image.PartitionImage.with_config('sdxx', '/tmp', self.BACKUPSET, config)
        self.assertTrue(logger_mock.error.called)

    def assert_config(self, imager, overwrite, rescue, space_check, fs_check,
                      crc_check, force, refresh_delay, compress):
        self.assertEqual(overwrite, imager.config['overwrite'])
        self.assertEqual(rescue, imager.config['rescue'])
        self.assertEqual(space_check, imager.config['space_check'])
        self.assertEqual(fs_check, imager.config['fs_check'])
        self.assertEqual(crc_check, imager.config['crc_check'])
        self.assertEqual(force, imager.config['force'])
        self.assertEqual(refresh_delay, imager.config['refresh_delay'])
        self.assertEqual(compress, imager.config['compress'])

    def test_config_to_command_parameters(self):
        clone = image.PartitionImage('sdxx','/tmp', self.BACKUPSET, overwrite=False, rescue=False,
                                     space_check=False, fs_check=False,
                                     crc_check=False, force=False,
                                     refresh_delay=0, compress=False)
        self.assertEqual('-C -I -i',
                         ' '.join(clone._config_to_command_parameters()))
        clone = image.PartitionImage('sdxx','/tmp', self.BACKUPSET, overwrite=False, rescue=True,
                                     space_check=True, fs_check=True,
                                     crc_check=True, force=True,
                                     refresh_delay=10, compress=False)
        self.assertEqual('-R -F -f 10',
                         ' '.join(clone._config_to_command_parameters()))

    def test_build_command(self):
        self.clone.config['overwrite'] = True
        self.assertEqual('partclone.ntfs -f 5 -s /source/file -O /target/file',
                         ' '.join(self.clone._build_command(self.source,
                                                            self.target, self.fs)))
        self.clone.config['overwrite'] = False
        self.assertEqual('partclone.ntfs -f 5 -s /source/file -o /target/file',
                         ' '.join(self.clone._build_command(self.source,
                                                            self.target, self.fs)))

    # TODO: fix runner factory tests.
    # def test_get_backup_runner_without_compression(self):
    #     partition_data = self.DETECT_MOCK_VALUES['sdxx']['partitions'][0]
    #     self.clone.config['compress'] = False
    #     self.clone._prepare_for_partition(partition_data)
    #     runner = self.clone._get_backup_runner()
    #     self.assertFalse('mksquashfs' in runner.command)

    # def test_get_backup_runner_with_compression(self):
    #     partition_data = self.DETECT_MOCK_VALUES['sdxx']['partitions'][0]
    #     self.clone.config['compress'] = True
    #     self.clone._prepare_for_partition(partition_data)
    #     runner = self.clone._get_backup_runner()
    #     self.assertTrue('mksquashfs' in runner.command)

    def test_backup_command(self):
        command = self.clone._backup_command(self.source, self.target, self.fs)
        self.assertTrue('-c' in command)
        self.assertFalse('-r' in command)

    def test_restore_command(self):
        command = self.clone._restore_command(self.source, self.target, self.fs)
        self.assertTrue('-r' in command)
        self.assertFalse('-c' in command)

    @patch('src.diskutils.image.Execute')
    def test_backup(self, exec_class):
        exec_mock = Mock()
        exec_mock.poll.return_value = 0
        exec_class.return_value = exec_mock
        exec_mock.output.return_value = {
            "completed": "100.00%",
            "elapsed": "00:00:15",
            "rate": "817.66mb/min",
            "remaining": "00:00:00",
        }
        self.clone.backup()
        self.assertEqual(1, exec_mock.run.call_count)
        self.assertEqual(1, exec_class.call_count)
        self.assertEqual(self.clone.STATUS_FINISHED, self.clone._status['sdxx1']['status'])

    @patch('src.diskutils.image.Execute')
    def test_backup_raises_on_error(self, execute_mock):
        runner = Mock()
        runner.run.side_effect = Exception('Something went wrong')
        execute_mock.return_value = runner
        with self.assertRaises(Exception):
            self.clone.backup()

    def test_handle_exit_code(self):
        self.clone._update_status = Mock()
        self.clone._current_partition = 'sdxx1'
        self.clone._handle_exit_code(0)
        self.assertEqual('finished', self.clone._status['sdxx1']['status'])
        self.assertTrue(self.clone._update_status.called)

    def test_handle_exit_code_with_error(self):
        self.clone._update_status = Mock()
        self.clone._current_partition = 'sdxx1'
        with self.assertRaises(Exception):
            self.clone._handle_exit_code(123)
            self.assertEqual('error', self.clone._status['sdxx1']['status'])
            self.assertFalse(self.clone._update_status.called)

    # TODO: Write new image restoration test.
    # def test_restore(self):
    #     with self.assertRaises(NotImplementedError):
    #         self.clone.restore()


class PartcloneOutputParserTest(unittest.TestCase):

    def setUp(self):
        self.parser = image._PartcloneOutputParser()

    @patch('src.diskutils.image.logging')
    def test_check_for_errors(self, log_mock):
        string = 'open target fail /tmp/part1.img: file exists (17)'
        with self.assertRaises(image.ImageError):
            self.parser._check_for_errors(string)
        self.assertEqual(1, log_mock.error.call_count)

    def test_parse(self):
        base_string = 'Elapsed: 00:01:22, Remaining: 00:01:20, Completed: 50.2%, '
        self._parse_and_assert(base_string + '816.85mb/min', '00:01:22',
                               '00:01:20', '50.2')
        self._parse_and_assert(base_string + 'Rate: 816.85MB/min, ',
                               '00:01:22', '00:01:20', '50.2')

    def test_valid_data_is_not_removed_by_empty_lines(self):
        self.parser.parse('')
        self.assertEqual(None, self.parser.output)
        string = 'Elapsed: 00:01:22, Remaining: 00:01:20, Completed: 50.2%, '
        self._parse_and_assert(string + '816.85mb/min', '00:01:22',
                               '00:01:20', '50.2')
        self._parse_and_assert('', '00:01:22', '00:01:20', '50.2')

    @patch('src.diskutils.image.logging')
    def test_parse_checks_for_errors(self, log_mock):
        with self.assertRaises(image.ImageError):
            self.parser.parse('open target fail /tmp/part1.img: file exists (17)')

    def _parse_and_assert(self, string, elapsed, remaining, completed):
        self.parser.parse(string)
        self.assertEqual({'elapsed': elapsed, 'remaining': remaining,
                          'completed': completed}, self.parser.output)
