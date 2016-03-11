"""Author: Oktawiusz Wilk
Date: 05/11/2015
"""
import logging
from os import path, remove

import constants
from lib.exceptions import ImageException, DiskSpaceException
from .backupset import Backupset
from .runcommand import OutputParser, Execute
from services.utils import BackupRemover

class PartitionImage:
    """A wrapper for the Open Source partition imaging tool partclone
    (http://partclone.org/) and read only compressed file system squashfs.
    This class can be used to setup, start and monitor imaging procedure for
    file systems supported by partclone project.
    """

    CURRENT_PARTITION = 'current_partition'
    DEVICE_PATH = '/dev/'

    _fs_to_command = {
        'ntfs': 'partclone.ntfs',
        'fat32': 'partclone.fat32',
        'fat16': 'partclone.fat16',
        'fat12': 'partclone.fat12',
        'vfat': 'partclone.vfat',
        'exfat': 'partclone.exfat',
        'ext2': 'partclone.ext2',
        'ext3': 'partclone.ext3',
        'ext4': 'partclone.ext4',
        'hfsplus': 'partclone.hfsp',
        'hfs+': 'partclone.hfsp',
        'hfs': 'partclone.dd',  # There's no support for the legacy HFS in partclone
        'raw': 'partclone.dd',
    }

    def __init__(self, disk: str, path: str, backupset: 'Backupset', overwrite: bool = False, rescue: bool = False,
                 space_check: bool = True, fs_check: bool = True, crc_check: bool = True,
                 force: bool = False, refresh_delay: int = 5, compress: bool = False):
        self.disk = disk
        self.backupset = backupset
        self.path = path
        self.config = {
            'overwrite': overwrite,
            'rescue': rescue,
            'space_check': space_check,
            'fs_check': fs_check,
            'crc_check': crc_check,
            'force': force,
            'refresh_delay': refresh_delay,
            'compress': compress
        }
        self._status = []
        self._current_partition = ""
        self._runner = None
        self._logger = logging.getLogger(__name__)
        self._init_status()

    @classmethod
    def with_config(cls, disk: str, path: str, backupset: 'Backupset', config: dict):
        try:
            return cls(disk, path, backupset, config['overwrite'], config['rescue'],
                       config['space_check'], config['fs_check'],
                       config['crc_check'], config['force'],
                       config['refresh_delay'], config['compress'])
        except BaseException as e:
            logging.getLogger(__name__).error('Cannot build imager with config ' + str(config) + ', reason: ' + str(e))
            raise e

    def get_status(self):
        """Returns updated status for the executed process."""
        self._update_status()
        return self._status

    def backup(self):
        """Creates image backup for each of the partitions on the designated drive"""
        for partition in self.backupset.partitions:
            self._prepare_partition_info(partition)
            self._runner = self._get_backup_runner()
            self._run_process()

    def restore(self):
        """Restores image backups to the designated drive"""
        for partition in self.backupset.partitions:
            self._prepare_partition_info(partition)
            self._runner = self._get_restoration_runner()
            self._run_process()

    def _prepare_partition_info(self, partition):
        self._current_partition = self.disk + partition.id
        self._current_device = self.DEVICE_PATH + self._current_partition
        self._current_image_file = self.path + constants.PARTITION_FILE_PREFIX + partition.id + \
                                   constants.PARTITION_FILE_SUFFIX
        self._current_fs = partition.file_system

    def _run_process(self):
        self._get_partition_status(self._current_partition)['status'] = constants.STATUS_RUNNING
        retry = True
        while retry:
            retry = False
            try:
                if path.exists(self._current_device):
                    self._runner.run()
                    self._handle_exit_code(self._runner.poll())
                else:
                    raise ImageException('The device ' + self._current_device + ' is unavailable.')
            except DiskSpaceException as e:
                BackupRemover.handle_space_error(e)
                print(str(self._current_image_file))
                if path.exists(self._current_image_file):
                    remove(self._current_image_file)
                retry = True
            except Exception as e:
                self._get_partition_status(self._current_partition)['status'] = constants.STATUS_ERROR
                raise Exception(
                        'Error detected during imaging partition: ' + self._current_partition + '. Cause: ' + str(e))

    def _init_status(self):
        """Initializes the status information with all partitions detected for
        the target disk. The status for each partition is set to pending."""
        for partition in self.backupset.partitions:
            self._status.append({
                'name': self.disk + partition.id,
                'status': constants.STATUS_PENDING,
                'completed': '0',
                'elapsed': '00:00:00',
                'remaining': '00:00:00',
            })

    def _update_status(self):
        """Retrieves newest output from output parser and includes it with the
        status information."""
        if self._current_partition and self._runner and self._runner.output():
            partition_status = self._get_partition_status(self._current_partition)
            partition_status.update(self._runner.output())
            partition_status['name'] = self._current_partition

    def _get_partition_status(self, target):
        for partition in self._status:
            if partition['name'] == target:
                return partition
        raise Exception

    def _get_backup_runner(self):
        if self.config['compress']:
            command = self._command_with_compression(self._current_device,
                                                     self._current_image_file,
                                                     self._current_fs)
            return Execute(' '.join(command), _PartcloneOutputParser(),
                           shell=True, use_pty=True)
        else:
            command = self._backup_command(self._current_device,
                                           self._current_image_file, self._current_fs)
            return Execute(command, _PartcloneOutputParser(), use_pty=True)

    def _get_restoration_runner(self):
        command = self._restore_command(self._current_image_file,
                                        self._current_device, self._current_fs)
        return Execute(command, _PartcloneOutputParser(), use_pty=True)

    def _backup_command(self, source: str, target: str, fs: str):
        """Creates a backup command for specified partition
        source - the partition to be imaged eg. /dev/sdb1
        target - file for partition image eg. /tmp/part1.img
        fs - filesystem to be imaged, this is used to select appropriate
             partclone version.
        """
        command = self._build_command(source, target, fs)
        command.append('-c')  # create backup
        return command

    def _command_with_compression(self, source: str, target: str, fs: str):
        TEMP_DIR = '/dev/null'
        image_name = target[target.rindex('/'):]
        return ['mksquashfs', TEMP_DIR, target.replace('img', 'sqfs'),
                '-noappend', '-no-progress', '-p \'' + image_name +
                ' f 444 root root ' +
                ' '.join(self._backup_command(source, '-', fs)) + '\'']

    def _restore_command(self, source: str, target: str, fs: str):
        """Creates a restore command for specified partition
        source - the file containing partition image eg. /tmp/part1.img
        target - the partition for the image to be applied to eg. /dev/sdb1
        fs - filesystem to be imaged, this is used to select appropriate
             partclone version.
        """
        command = self._build_command(source, target, fs)
        command.append('-r')  # restore backup
        return command

    def _build_command(self, source: str, target: str, fs: str):
        """Builds the generic part of the partclone command."""
        command = list()
        command.append(self._select_command_by_fs(fs))
        command.extend(self._config_to_command_parameters())
        command.extend(['-s', source])
        if self.config['overwrite']:
            command.extend(['-O', target])
        else:
            command.extend(['-o', target])
        return command

    def _select_command_by_fs(self, fs: str):
        """Selects appropriate partclone version for the filesystem.
        Unrecognised filesystems will be treated as raw images and supported
        with use of partclone.dd
        """
        if fs not in self._fs_to_command:
            fs = 'raw'
        return self._fs_to_command[fs]

    def _config_to_command_parameters(self):
        """Parses configuration initialised when this class is created into the
        specific partclone switches.
        """
        command = list()
        if self.config['rescue']:
            command.append('-R')
        if not self.config['space_check']:
            command.append('-C')
        if not self.config['fs_check']:
            command.append('-I')
        if not self.config['crc_check']:
            command.append('-i')
        if self.config['force']:
            command.append('-F')
        if self.config['refresh_delay']:
            command.extend(['-f', str(self.config['refresh_delay'])])
        return command

    def _handle_exit_code(self, exit_code):
        partition_details = self._get_partition_status(self._current_partition)
        if exit_code == 0:
            self._update_status()
            partition_details['status'] = constants.STATUS_FINISHED
        else:
            partition_details['status'] = constants.STATUS_ERROR
            raise Exception('The imaging did not finish successfully. (Code: ' + str(exit_code) + ')')


class _PartcloneOutputParser(OutputParser):
    _VALID_KEYS = ['elapsed', 'remaining', 'completed']
    ERROR_MAPPING = {
        "destination doesn't have enough free space": 'Not enough free space on the destination disk.',
        "file exists (17)": 'Image file already exists, if you want to replace backup, make sure to check the overwrite option.',
        "*** buffer overflow detected ***:": 'Imaging software caused buffer overflow. Check if source disk is still present in the system.',
        "failed to read file": 'Selected backup cannot be read.',
        "use the --rescue option": 'I/O errors detected, if you wish to continue, restart backup with the rescue option selected.',
        "or fix it by fsck": 'A file system is marked as dirty on the source disk. Fix the file system and try again or disable the filesystem check option.',
        "use option -c to disable size checking(dangerous)": 'Target disk is smaller than the original. Use larger disk or disable space checking.',
        "error": 'An unknown error was caused by imaging software.'
    }

    def __init__(self):
        self.output = None
        self._output_dict = {}
        self._logger = logging.getLogger(__name__)
        self._skip = True

    def parse(self, data):
        """Processes data from the command output and saves the result as output."""
        raw_output = data.replace("\x1b[A", "").lower()
        str_out = str(raw_output).strip()
        if 'remaining:' not in str_out and 'complete:' not in str_out:
            self._logger.debug(str(raw_output))
        self._check_for_errors(raw_output)
        if not self._skip:
            raw_output = raw_output.split(',')
            for item in raw_output:
                if ':' in item:
                    key, value = item.lower().split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    if key in self._VALID_KEYS and 'completed' in key:
                        self._output_dict[key] = value[0:-1]
                    elif key in self._VALID_KEYS:
                        self._output_dict[key] = value
            if self._output_dict:
                self.output = self._output_dict
        else:
            if 'file system:' in str_out:
                self._skip = False

    def _check_for_errors(self, string):
        """Tests the output string for error messages and processes them."""
        string = string.lower()
        if "destination doesn't have enough free space" in string:
            raise DiskSpaceException(string)
        for error in self.ERROR_MAPPING:
            self._find_and_raise(string, error, message=self.ERROR_MAPPING[error])

    def _find_and_raise(self, string, target, message):
        if target in string:
            self._logger.error(string)
            raise ImageException(message)