"""Author: Oktawiusz Wilk
Date: 06/11/2015
"""
import logging
from os import path
from .runcommand import Execute, OutputToFileConverter


class DiskLayout:
    def __init__(self, disk, target_dir, overwrite:bool=False):
        self.disk = disk
        self.target_dir = target_dir
        self.overwrite = overwrite

    @classmethod
    def with_config(cls, disk, target_dir, config):
        try:
            return cls(disk, target_dir, config['overwrite'])
        except BaseException as e:
            logging.error('Cannot build DiskLayout with config ' + str(config) + ', reason: ' + str(e))
            raise e

    def detect_layout(self):
        command = 'parted /dev/' + self.disk + ' p | grep "Partition Table"'
        parted = Execute(command, shell=True)
        parted.run()
        key, value = parted.output().split(':')
        if 'msdos' in value:
            return 'MBR'
        elif 'gpt' in value:
            return 'GPT'
        else:
            return 'UNKNOWN'

    def backup_layout(self):
        layout = self.detect_layout()
        if layout is 'MBR':
            self._backup_mbr_layout()
        elif layout is 'GPT':
            self._backup_gpt_layout()
        else:
            raise Exception("Unrecognized partition layout detected.")

    def _backup_mbr_layout(self):
        self._backup_mbr()
        self._backup_mbr_partition_table()

    def _backup_gpt_layout(self):
        raise NotImplementedError

    def _restore_mbr_layout(self):
        raise NotImplementedError

    def _restore_gpt_layout(self):
        raise NotImplementedError

    def _backup_mbr(self):
        mbr_size = 512
        target_file = self.target_dir + 'mbr.img'
        self._check_if_file_exists_with_raise(target_file,
            'Existing MBR backup detected at ' + target_file + '. Not overwritting.')
        dd_command = ['dd', 'if=/dev/' + self.disk, 'of=' + target_file, 'bs=' +
                     str(mbr_size), 'count=1']
        dd = Execute(dd_command)
        if(dd.run() != 0):
            logging.error('MBR backup failed, disk:' + self.disk + ', target:' +
                          self.target_dir)
        if not path.exists(target_file):
            raise Exception('MBR backup file not created!')

    def _backup_mbr_partition_table(self):
        sfdisk_command = ['sfdisk', '-d', '/dev/' + self.disk]
        target_file = self.target_dir + 'ptable.bak'
        self._check_if_file_exists_with_raise(target_file,
            'Existing backup detected at ' + target_file + '. Not overwritting.')
        sfdisk = Execute(sfdisk_command, OutputToFileConverter(target_file))
        if(sfdisk.run() != 0):
            logging.error('Partition table backup failed, disk:' + self.disk +
                          ', target:' + self.target_dir)
        if not path.exists(target_file):
            raise Exception('Partition layout backup not created!')

    def _check_if_file_exists_with_raise(self, file, error_message):
        if path.exists(file) and not self.overwrite:
            logging.error(error_message)
            raise Exception(error_message)
