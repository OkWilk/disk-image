import unittest
from unittest.mock import Mock, patch
import src.systools.image as image


class ImageTest(unittest.TestCase):

    @patch('src.systools.image.detect_disks')
    def setUp(self, detect_mock):
        detect_mock.return_value = {'sda': {'partitions':
            [{'fs': 'ntfs', 'name': 'sda1', 'size': '3930423296'}],
            'size': '3932160000', 'type': 'disk'}}
        self.clone = image.PartitionImage('sda', '/tmp/')
        self.source = '/source/file'
        self.target = '/target/file'
        self.fs = 'ntfs'

    def test_core_fs_are_supported(self):  # TODO: add HFS+
        self.assertEqual('partclone.ntfs', self.clone._fs_to_command['ntfs'])
        self.assertEqual('partclone.vfat', self.clone._fs_to_command['vfat'])
        self.assertEqual('partclone.ext3', self.clone._fs_to_command['ext3'])
        self.assertEqual('partclone.ext4', self.clone._fs_to_command['ext4'])
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
        self.assertTrue(self.clone.CURRENT_PARTITION in self.clone._status)
        self.assertTrue('sda1' in self.clone._status)
        self.assertEqual(self.clone.STATUS_PENDING, self.clone._status['sda1']['status'])

    def test_get_status(self):
        self.assertTrue('sda1' in self.clone.get_status())
        self.assertTrue(self.clone.CURRENT_PARTITION in self.clone.get_status())

    @patch('src.systools.image.detect_disks')
    def test_config_to_command_parameters(self, detect_mock):
        detect_mock.return_value = {'sda': {'partitions':
            [{'fs': 'ntfs', 'name': 'sda1', 'size': '3930423296'}],
            'size': '3932160000', 'type': 'disk'}}
        clone = image.PartitionImage('sda','/tmp', overwrite=False, rescue=False,
                                     space_check=False, fs_check=False,
                                     crc_check=False, force=False,
                                     refresh_delay=0, verbose=False)
        self.assertEqual('-C -I -i -B',
                         ' '.join(clone._config_to_command_parameters()))
        clone = image.PartitionImage('sda','/tmp', overwrite=False, rescue=True,
                                     space_check=True, fs_check=True,
                                     crc_check=True, force=True,
                                     refresh_delay=10, verbose=True)
        self.assertEqual('-R -F -f 10',
                         ' '.join(clone._config_to_command_parameters()))

    def test_build_command(self):
        self.clone.config['overwrite'] = True
        self.assertEqual('partclone.ntfs -f 5 -B -s /source/file -O /target/file',
                         ' '.join(self.clone._build_command(self.source,
                                                            self.target, self.fs)))
        self.clone.config['overwrite'] = False
        self.assertEqual('partclone.ntfs -f 5 -B -s /source/file -o /target/file',
                         ' '.join(self.clone._build_command(self.source,
                                                            self.target, self.fs)))

    def test_backup_command(self):
        command = self.clone._backup_command(self.source, self.target, self.fs)
        self.assertTrue('-c' in command)
        self.assertFalse('-r' in command)

    def test_restore_command(self):
        command = self.clone._restore_command(self.source, self.target, self.fs)
        self.assertTrue('-r' in command)
        self.assertFalse('-c' in command)

    @patch('src.systools.image.Execute')
    def test_backup(self, exec_class):
        exec_mock = Mock()
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
        self.assertEqual(self.clone.STATUS_FINISHED, self.clone._status['sda1']['status'])

    def test_restore(self):
        with self.assertRaises(NotImplementedError):
            self.clone.restore()


class PartcloneOutputParserTest(unittest.TestCase):

    def setUp(self):
        self.parser = image._PartcloneOutputParser()

    @patch('src.systools.image.logging')
    def test_check_for_errors(self, log_mock):
        string = 'open target fail /tmp/part1.img: file exists (17)'
        with self.assertRaises(image.ImageError):
            self.parser._check_for_errors(string)
        self.assertEqual(1, log_mock.error.call_count)

    def test_parse(self):
        base_string = 'Elapsed: 00:01:22, Remaining: 00:01:20, Completed: 50.20%,'
        self._parse_and_assert(base_string + '816.85mb/min', '00:01:22',
                               '00:01:20', '50.20%', '816.85mb/min')
        self._parse_and_assert(base_string + 'Rate: 816.85MB/min, ',
                               '00:01:22', '00:01:20', '50.20%', '816.85mb/min')

    @patch('src.systools.image.logging')
    def test_parse_checks_for_errors(self, log_mock):
        with self.assertRaises(image.ImageError):
            self.parser.parse('open target fail /tmp/part1.img: file exists (17)')

    def _parse_and_assert(self, string, elapsed, remaining, completed, rate):
        self.parser.parse(string)
        self.assertEqual({'elapsed': elapsed, 'remaining': remaining,
                          'completed': completed, 'rate': rate}, self.parser.output)
