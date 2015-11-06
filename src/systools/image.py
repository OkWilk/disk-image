"""Author: Oktawiusz Wilk
Date: 05/11/2015
"""
import logging
from src.systools.diskdetect import detect_disks
from src.systools.runcommand import OutputParser, Execute
from pprint import pprint


class PartitionImage:
    """A wrapper for the Open Source partition imaging tool partclone
    (http://partclone.org/) and read only compressed file system squashfs.
    This class can be used to setup, start and monitor imaging procedure for
    file systems supported by partclone project.
    """

    _fs_to_command = {  # TODO: add remaining filesystems (!HFS+!)
        'ntfs': 'partclone.ntfs',
        'fat32': 'partclone.fat32',
        'fat16': 'partclone.fat16',
        'fat12': 'partclone.fat12',
        'vfat': 'partclone.vfat',
        'exfat': 'partclone.exfat',
        'ext2': 'partclone.ext2',
        'ext3': 'partclone.ext3',
        'ext4': 'partclone.ext4',
        'raw': 'partclone.dd',
    }

    def __init__(self, disk:str, dir:str, overwrite:bool=False, rescue:bool=False,
                 space_check:bool=True, fs_check:bool=True, crc_check:bool=True,
                 force:bool=False, refresh_delay:int=5, verbose:bool=False):
        self.disk = disk
        self.disk_info = self._get_disk_info(disk)
        # TODO: add exception if disk info cannot be retrieved
        self.dir = dir
        self.config = {
            'overwrite': overwrite,
            'rescue': rescue,
            'space_check': space_check,
            'fs_check': fs_check,
            'crc_check': crc_check,
            'force': force,
            'refresh_delay': refresh_delay,
            'verbose': verbose,
        }

    def _get_disk_info(self, disk:str):
        """Retrieves information regarding the specified disk."""
        return detect_disks()[disk]

    def backup(self):
        """Creates image backup for each of the partitions on the designated drive"""
        for partition in self.disk_info['partitions']:
            source = '/dev/' + partition['name']
            target = self.dir + partition['name'].replace(self.disk, 'part') + '.img'
            fs = partition['fs']
            command = self._backup_command(source, target, fs)
            runner = Execute(command, _PartcloneOutputParser(), use_pty=True)
            runner.run()  # TODO: raise error in case of any issues

    def restore(self):
        """Restores image backups to the designated drive"""
        raise NotImplementedError

    def _backup_command(self, source:str, target:str, fs:str):
        """Creates a backup command for specified partition
        source - the partition to be imaged eg. /dev/sdb1
        target - file for partition image eg. /tmp/part1.img
        fs - filesystem to be imaged, this is used to select appropriate
             partclone version.
        """
        command = self._build_command(source, target, fs)
        command.append('-c')  # create backup
        return command

    def _restore_command(self, source:str, target:str, fs:str):
        """Creates a restore command for specified partition
        source - the file containing partition image eg. /tmp/part1.img
        target - the partition for the image to be applied to eg. /dev/sdb1
        fs - filesystem to be imaged, this is used to select appropriate
             partclone version.
        """
        command = self._build_command(source, target, fs)
        command.append('-r')  # restore backup
        return command

    def _build_command(self, source:str, target:str, fs:str):
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

    def _select_command_by_fs(self, fs:str):
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
        if not self.config['verbose']:
            command.append('-B')
        return command


class _PartcloneOutputParser(OutputParser):
    _valid_keys = ['elapsed', 'remaining', 'completed', 'rate']

    def __init__(self):
        self.output = None
        self._output_dict = {}

    def parse(self, data):
        """Processes data from the command output and saves the result as output."""
        raw_output = data.replace("\x1b[A","").lower()
        self._check_for_errors(raw_output)
        raw_output = raw_output.split(',')
        for item in raw_output:
                if ':' in item:
                    key, value = item.lower().split(':',1)
                    key = key.strip()
                    value = value.strip()
                    if key in self._valid_keys:
                        self._output_dict[key] = value
                elif '/min' in item:
                    self._output_dict['rate'] = item.strip()
        if self._output_dict:
            self.output = self._output_dict

    def _check_for_errors(self, string):
        """Tests the output string for error messages and processes them."""
        string = string.lower()
        if 'file exists (17)' in string:
            logging.error(string)
            raise ImageError(string)


class ImageError(Exception):
    """Raised in case of backup and restoration issues."""
    pass
