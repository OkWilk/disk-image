"""Author: Oktawiusz Wilk
Date: 06/11/2015
"""
import logging
from os import path
from .runcommand import Execute, OutputToFileConverter


class DiskLayout:
    def __init__(self):
        pass

    def backup_layout(self, disk, target_dir):
        layout = self.detect_layout(disk)
        if layout is 'MBR':
            self._backup_mbr_layout(disk, target_dir)
        elif layout is 'GPT':
            self._backup_gpt_layout(disk, target_dir)
        else:
            raise Exception("Unrecognized partition layout detected.")

    def detect_layout(self, disk):
        command = 'parted /dev/' + disk + ' p | grep "Partition Table"'
        parted = Execute(command, shell=True)
        parted.run()
        key, value = parted.output().split(':')
        if 'msdos' in value:
            return 'MBR'
        elif 'gpt' in value:
            return 'GPT'
        else:
            return 'UNKNOWN'

    def _backup_mbr_layout(self, disk, target_dir):
        self._backup_mbr(disk, target_dir)
        self._backup_mbr_partition_table(disk, target_dir)

    def _backup_gpt_layout(self):
        raise NotImplementedError

    def _restore_mbr_layout(self):
        raise NotImplementedError

    def _restore_gpt_layout(self):
        raise NotImplementedError

    def _backup_mbr(self, disk, target_dir):
        mbr_size = 512
        target_file = target_dir + 'mbr.img'
        dd_command = ['dd', 'if=/dev/' + disk, 'of=' + target_file, 'bs=' +
                     str(mbr_size), 'count=1']
        dd = Execute(dd_command)
        if(dd.run() != 0):
            logging.error('MBR backup failed, disk:' + disk + ', target:' +
                          target_dir)
        if not path.exists(target_file):
            raise Exception('MBR backup file not created!')

    def _backup_mbr_partition_table(self, disk, target_dir):
        sfdisk_command = ['sfdisk', '-d', '/dev/' + disk]
        sfdisk = Execute(sfdisk_command, OutputToFileConverter(target_dir +
                         'ptable.bak'))
        if(sfdisk.run() != 0):
            logging.error('Partition table backup failed, disk:' + disk +
                          ', target:' + target_dir)
            # TODO: check if file has been created and if size is not 0
            raise Exception()  # TODO: replace with more meaningful exception
