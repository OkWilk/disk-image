"""Author: Oktawiusz Wilk
Date: 06/11/2015
"""
import logging
from abc import ABCMeta, abstractmethod
from os import path

import constants
from lib.exceptions import DetectionException
from .runcommand import Execute, OutputToFileConverter


class DiskLayout:
    def __init__(self, disk, target_dir, layout=None, overwrite: bool = False):
        self.disk = disk
        self.target_dir = target_dir
        self.overwrite = overwrite
        self._layout_factory = LayoutManagerFactory()
        self._layout_manager = self._layout_factory.get_layout_manager(disk, target_dir, layout, overwrite)

    @classmethod
    def with_config(cls, disk, target_dir, config, layout=None):
        try:
            return cls(disk, target_dir, layout=layout, overwrite=config['overwrite'])
        except BaseException as e:
            logging.getLogger(__name__).error('Cannot build DiskLayout with config ' + str(config) + ', reason: ' + str(e))
            raise e

    def get_layout(self):
        return self._layout_manager.layout

    def backup_layout(self):
        self._check_if_disk_exists_with_raise()
        self._layout_manager.backup_layout()

    def restore_layout(self):
        self._check_if_disk_exists_with_raise()
        self._layout_manager.restore_layout()

    def _check_if_disk_exists_with_raise(self):
        if not path.exists('/dev/' + self.disk):
            raise Exception('The disk ' + self.disk + ' is unavailable.')


class LayoutManagerFactory:
    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def get_layout_manager(self, disk, target_dir, layout, overwrite):
        if not layout:
            layout = self._detect_layout(disk)
        if 'MBR' in layout:
            return MBRLayoutManager(disk, target_dir, overwrite)
        elif 'GPT' in layout:
            return GPTLayoutManager(disk, target_dir, overwrite)
        else:
            raise ValueError("Unsupported or invalid disk layout requested.")

    def _detect_layout(self, disk):
        command = 'parted /dev/' + disk + ' p | grep "Partition Table\|Error"'
        parted = Execute(command, shell=True)
        parted.run()
        if 'Error' in parted.output():
            self._logger.warning('Could not detect partition layout for the device /dev/' + disk +
                                 '. Cause: ' + parted.output())
            raise DetectionException('Cannot detect partition layout for the ' + disk + '.')
        key, value = parted.output().split(':')
        if 'msdos' in value:
            return 'MBR'
        elif 'gpt' in value:
            return 'GPT'
        else:
            return 'UNKNOWN'


class LayoutManager:
    __metaclass__ = ABCMeta
    MBR_SIZE = 512
    MBR_TARGET_FILE = 'mbr.img'
    MAX_GPT_BACKUP_SIZE = 17408  # Formula: (128 * n) + 1024, where n is a max number of partitions in GPT (128)
    PARTITION_TABLE_TARGET_FILE = 'ptable.bak'

    def __init__(self, disk, target_dir, overwrite):
        self.layout = None
        self.disk = disk
        self.target_dir = target_dir
        self.overwrite = overwrite
        self._logger = logging.getLogger(__name__)

    @abstractmethod
    def backup_layout(self):
        pass

    @abstractmethod
    def restore_layout(self):
        pass

    def _remove_previous_partition_tables(self):
        self.__remove_primary_partition_table()
        self.__remove_backup_partition_table()

    def _refresh_partition_table(self):
        command = ['partprobe', '/dev/' + self.disk]
        partprobe = Execute(command)
        if partprobe.run() != 0:
            self._logger.error("Cannot refresh partition table for the disk: " + self.disk)

    def _check_if_file_exists_with_raise(self, file, error_message):
        if path.exists(file) and not self.overwrite:
            self._logger.error(error_message)
            raise Exception(error_message)

    def __remove_primary_partition_table(self):
        dd_command = ['dd', 'if=/dev/zero', 'of=/dev/' + self.disk, 'bs=' + str(self.MAX_GPT_BACKUP_SIZE), 'count=1']
        runner = Execute(dd_command)
        if runner.run() != 0:
            self._logger.warning('Cannot remove partition table at the start of the disk: /dev/' + self.disk + '.')

    def __remove_backup_partition_table(self):
        command = ['blockdev', '--getsz', '/dev/' + self.disk]
        runner = Execute(command)
        if runner.run() != 0:
            self._logger.warning('Could not retrieve disk size in blocks for disk: /dev/' + self.disk + '.')
        else:
            size = int(runner.output()) - 1024
            dd_command = ['dd', 'if=/dev/zero', 'of=/dev/' + self.disk, 'bs=512', 'count=1024', 'seek=' + str(size)]
            runner = Execute(dd_command)
            if runner.run() != 0:
                self._logger.warning('Cannot remove backup partition table at the end on the disk: /dev/' + self.disk + '.')


class MBRLayoutManager(LayoutManager):
    def __init__(self, disk, target_dir, overwrite):
        super(MBRLayoutManager, self).__init__(disk, target_dir, overwrite)
        self.layout = 'MBR'

    def backup_layout(self):
        self._backup_mbr()
        self._backup_mbr_partition_table()

    def restore_layout(self):
        self._remove_previous_partition_tables()
        self._restore_mbr()
        self._restore_mbr_partition_table()
        self._refresh_partition_table()

    def _backup_mbr(self):
        target_file = self.target_dir + constants.BOOT_RECORD_FILE
        self._check_if_file_exists_with_raise(target_file,
                                              'Existing MBR backup detected at ' + target_file + '. Not overwritting.')
        dd_command = ['dd', 'if=/dev/' + self.disk, 'of=' + target_file, 'bs=' +
                      str(self.MBR_SIZE), 'count=1']
        dd = Execute(dd_command)
        if dd.run() != 0:
            self._logger.error('MBR backup failed, disk:' + self.disk + ', target:' +
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
        if sfdisk.run() != 0:
            self._logger.error('Partition table backup failed, disk:' + self.disk +
                          ', target:' + self.target_dir)
            raise Exception('Partition layout backup did not finish successfully.')
        if not path.exists(target_file):
            raise Exception('Partition layout backup not created!')

    def _restore_mbr(self):
        source_file = self.target_dir + constants.BOOT_RECORD_FILE
        if not path.exists(source_file):
            raise Exception('MBR backup is missing.')
        dd_command = ['dd', 'if=' + source_file, 'of=/dev/' + self.disk]
        dd = Execute(dd_command)
        if dd.run() != 0:
            self._logger.error('MBR restoration failed, source: ' + source_file +
                          ', target: ' + self.disk)

    def _restore_mbr_partition_table(self):
        source_file = self.target_dir + constants.PARTITION_TABLE_FILE
        if not path.exists(source_file):
            raise Exception('Partition layout backup is missing.')
        sfdisk_command = "sfdisk -f /dev/" + self.disk + ' < ' + source_file
        sfdisk = Execute(sfdisk_command, shell=True)
        if sfdisk.run() != 0:
            self._logger.error("Partition table restoration failed, source: " +
                          source_file + ', target: ' + self.disk)


class GPTLayoutManager(LayoutManager):
    def __init__(self, disk, target_dir, overwrite):
        super(GPTLayoutManager, self).__init__(disk, target_dir, overwrite)
        self.layout = 'GPT'

    def backup_layout(self):
        self._backup_boot_sector()
        self._backup_guid_partition_table()

    def restore_layout(self):
        self._remove_previous_partition_tables()
        self._restore_boot_sector()
        self._restore_guid_partition_table()
        self._refresh_partition_table()

    def _backup_boot_sector(self):
        target_file = self.target_dir + constants.BOOT_RECORD_FILE
        self._check_if_file_exists_with_raise(target_file,
                                              'Existing boot record backup detected at ' + target_file + '. Not overwritting.')
        dd_command = ['dd', 'if=/dev/' + self.disk, 'of=' + target_file, 'bs=' +
                      str(self.MAX_GPT_BACKUP_SIZE), 'count=1']
        dd = Execute(dd_command)
        if dd.run() != 0:
            self._logger.error('GPT backup failed, disk:' + self.disk + ', target:' +
                          self.target_dir)
            raise Exception('GPT backup did not finish successfully.')
        if not path.exists(target_file):
            raise Exception('GPT backup file not created!')

    def _backup_guid_partition_table(self):
        target_file = self.target_dir + constants.PARTITION_TABLE_FILE
        backup_command = ['sgdisk', '-b', target_file, '/dev/' + self.disk]
        self._check_if_file_exists_with_raise(target_file,
                                              'Existing backup detected at ' + target_file + '. Not overwritting.')
        sgdisk = Execute(backup_command)
        if sgdisk.run() != 0:
            self._logger.error('Partition table backup failed, disk:' + self.disk +
                          ', target:' + self.target_dir)
            raise Exception('Partition layout backup did not finish successfully.')
        if not path.exists(target_file):
            raise Exception('Partition layout backup not created!')

    def _restore_boot_sector(self):
        source_file = self.target_dir + constants.BOOT_RECORD_FILE
        if not path.exists(source_file):
            raise Exception('GPT backup is missing.')
        dd_command = ['dd', 'if=' + source_file, 'of=/dev/' + self.disk]
        dd = Execute(dd_command)
        if dd.run() != 0:
            self._logger.error('GPT restoration failed, source: ' + source_file +
                          ', target: ' + self.disk)

    def _restore_guid_partition_table(self):
        source_file = self.target_dir + constants.PARTITION_TABLE_FILE
        if not path.exists(source_file):
            raise Exception('Partition layout backup is missing.')
        sgdisk_command = ['sgdisk', '-l', source_file, '/dev/' + self.disk]
        sgdisk = Execute(sgdisk_command)
        if sgdisk.run() != 0:
            self._logger.error("Partition table restoration failed, source: " +
                          source_file + ', target: ' + self.disk)
