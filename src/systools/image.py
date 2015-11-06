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

    def __init__(self, disk:str, dir:str, overwrite=False, rescue=False,
                 space_check=True, fs_check=True, crc_check=True, force=False,
                 refresh_delay=5, verbose=False):
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
        for partition in self.disk_info['partitions']:
            source = '/dev/' + partition['name']
            target = self.dir + partition['name'].replace(self.disk, 'part') + '.img'
            fs = partition['fs']
            command = self._backup_command(source, target, fs)
            runner = Execute(command, PartcloneOutputParser(), use_pty=True)
            runner.run()

    def restore(self):
        raise NotImplementedError

    def _backup_command(self, source:str, target:str, fs:str):
        command = self._build_command(source, target, fs)
        command.append('-c')  # create backup
        return command

    def _restore_command(self, source:str, target:str, fs:str):
        command = self._build_command(source, target, fs)
        command.append('-r')  # restore backup
        return command

    def _build_command(self, source:str, target:str, fs:str):
        command = list()
        command.append(self._select_command_by_fs(fs))
        command.extend(self._config_to_command_parameters())
        command.extend(['-s', source])
        if self.config['overwrite']:
            command.extend(['-O', target])
        else:
            command.extend(['-o', target])
        return command

    def _select_command_by_fs(self, fs) -> str:
        if fs not in self._fs_to_command:
            fs = 'raw'
        return self._fs_to_command[fs]

    def _config_to_command_parameters(self) -> list:
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


class PartcloneOutputParser(OutputParser):
    _valid_keys = ['elapsed', 'remaining', 'completed', 'rate']

    def __init__(self):
        self.output = None
        self.output_dict = {}

    def parse(self, data):
        raw_output = data.replace("\x1b[A","").lower()
        self._check_for_errors(raw_output)
        raw_output = raw_output.split(',')
        for item in raw_output:
                if ':' in item:
                    key, value = item.lower().split(':',1)
                    key = key.strip()
                    value = value.strip()
                    if key in self._valid_keys:
                        self.output_dict[key] = value
                elif '/min' in item:
                    self.output_dict['rate'] = item.strip()
        if self.output_dict:
            self.output = self.output_dict
            pprint(self.output)

    def _check_for_errors(self, string):
        string = string.lower()
        if 'file exists (17)' in string:
            raise ImageError(string)
            logging.error(string)


class ImageError(Exception):
    pass
