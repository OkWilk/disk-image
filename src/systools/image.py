"""Author: Oktawiusz Wilk
Date: 05/11/2015
"""
from src.systools.diskdetect import detect_disks
from src.systools.runcommand import OutputParser, Execute
from pprint import pprint


class PartitionImage:
    """A wrapper for the Open Source partition imaging tool partclone
    (http://partclone.org/) and read only compressed file system squashfs.
    This class can be used to setup, start and monitor imaging procedure for
    file systems supported by partclone project.
    """

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
        'raw': 'partclone.dd',
    }

    def __init__(self, disk:str, dir:str, overwrite=False, log=False,
                 rescue=False, space_check=True, fs_check=True, crc_check=True,
                 force=False, refresh_delay=5, verbose=False):
        self.disk = disk
        self.disk_info = self._get_disk_info(disk)
        pprint(self.disk_info)
        self.dir = dir
        self.config = {
            'overwrite': overwrite,
            'log': log,
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

    def backup(self, path='/tmp/'):
        for partition in self.disk_info['partitions']:
            source = '/dev/' + partition['name']
            target = path + partition['name'].replace(self.disk, 'part') + '.img'
            fs = partition['fs']
            command = self._build_command(source, target, fs, backup=True)
            print(command)
            runner = Execute(command, PartcloneOutputParser(), use_pty=True)
            runner.run()

    def _build_command(self, source:str, target:str, fs:str, backup:bool):
        command = list()
        if fs not in self._fs_to_command:
            fs = 'raw'
        command.append(self._fs_to_command[fs])
        if backup:
            command.append('-c')  # create backup
        else:
            command.append('-r')  # restore backup
        command.extend(['-s', source])
        if self.config['overwrite']:
            command.extend(['-O', target])
        else:
            command.extend(['-o', target])
        if self.config['log'] and backup:
            command.extend(['-L', target + '.log'])
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

    def restore(self):
        pass


class PartcloneOutputParser(OutputParser):
    def __init__(self):
        self.output = None

    def parse(self, data):
        temp = data.replace("\x1b[A","")
        temp = "".join(temp.split())
        temp = temp.split(',')
        output_dict = {}
        for item in temp:
                if ':' in item:
                    key, value = item.lower().split(':',1)
                    output_dict[key.strip()] = value.strip()
        if output_dict:
            self.output = output_dict
            pprint(self.output)
