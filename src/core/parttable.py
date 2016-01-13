"""Author: Oktawiusz Wilk
Date: 06/11/2015
"""
import logging
import constants
from os import path
from .runcommand import Execute, OutputToFileConverter


class DiskLayout:
    MBR_SIZE = 512
    MBR_TARGET_FILE = 'mbr.img'
    PARTITION_TABLE_TARGET_FILE = 'ptable.bak'

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
        self._check_if_disk_exists_with_raise()
        layout = self.detect_layout()
        if layout == 'MBR':
            self._backup_mbr_layout()
        elif layout == 'GPT':
            self._backup_gpt_layout()
        else:
            raise Exception("Unrecognized partition layout detected.")

    def restore_layout(self, layout):
        self._check_if_disk_exists_with_raise()
        if layout == 'MBR':
            self._restore_mbr_layout()
        elif layout == 'GPT':
            self._restore_gpt_layout()
        else:
            raise Exception("Unrecognized partition layout detected.")

    def _backup_mbr_layout(self):
        self._backup_mbr()
        self._backup_mbr_partition_table()

    def _backup_gpt_layout(self):
        raise NotImplementedError

    def _restore_mbr_layout(self):
        self._restore_mbr()
        self._restore_mbr_partition_table()
        # self._refresh_partition_table()

    def _restore_gpt_layout(self):
        raise NotImplementedError

    def _backup_mbr(self):
        target_file = self.target_dir + constants.BOOT_RECORD_FILE
        self._check_if_file_exists_with_raise(target_file,
            'Existing MBR backup detected at ' + target_file + '. Not overwritting.')
        dd_command = ['dd', 'if=/dev/' + self.disk, 'of=' + target_file, 'bs=' +
                     str(self.MBR_SIZE), 'count=1']
        dd = Execute(dd_command)
        if(dd.run() != 0):
            logging.error('MBR backup failed, disk:' + self.disk + ', target:' +
                          self.target_dir)
            raise Exception('MBR backup did not finish successfully.')
        if not path.exists(target_file):
            raise Exception('MBR backup file not created!')

    def _backup_mbr_partition_table(self):
        sfdisk_command = ['sfdisk', '-d', '/dev/' + self.disk]
        target_file = self.target_dir + constants.PARTITION_TABLE_FILE
        self._check_if_file_exists_with_raise(target_file,
            'Existing backup detected at ' + target_file + '. Not overwritting.')
        sfdisk = Execute(sfdisk_command, OutputToFileConverter(target_file))
        if(sfdisk.run() != 0):
            logging.error('Partition table backup failed, disk:' + self.disk +
                          ', target:' + self.target_dir)
            raise Exception('Partition layout backup did not finish successfully.')
        if not path.exists(target_file):
            raise Exception('Partition layout backup not created!')

    def _check_if_disk_exists_with_raise(self):
        if not path.exists('/dev/' + self.disk):
            raise Exception('The disk ' + self.disk + ' is unavailable.')

    def _check_if_file_exists_with_raise(self, file, error_message):
        if path.exists(file) and not self.overwrite:
            logging.error(error_message)
            raise Exception(error_message)

    def _restore_mbr(self):
        source_file = self.target_dir + constants.BOOT_RECORD_FILE
        if not path.exists(source_file):
            raise Exception('MBR backup is missing.')
        dd_command = ['dd', 'if=' + source_file, 'of=/dev/' + self.disk]
        dd = Execute(dd_command)
        if(dd.run() != 0):
            logging.error('MBR restoration failed, source: ' + source_file +
                          ', target: ' + self.disk)

    def _restore_mbr_partition_table(self):
        source_file = self.target_dir + constants.PARTITION_TABLE_FILE
        if not path.exists(source_file):
            raise Exception('Partition layout backup is missing.')
        sfdisk_command = "sfdisk -f /dev/" + self.disk + ' < ' + source_file
        sfdisk = Execute(sfdisk_command, shell=True)
        if(sfdisk.run() != 0):
            logging.error("Partition table restoration failed, source: " +
                          source_file + ', target: ' + self.disk)

    def _refresh_partition_table(self):
        command = ['partprobe', '/dev/' + self.disk]
        partprobe = Execute(command)
        if(partprobe.run() != 0):
            logging.error("Cannot refresh partition table for the disk: " + self.disk)
