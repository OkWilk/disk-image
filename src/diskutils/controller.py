"""Author: Oktawiusz Wilk
Date: 30/11/2015
"""
import logging
import constants
from ast import literal_eval
from abc import ABCMeta, abstractmethod
from threading import Thread
from os import path, makedirs, listdir
from time import sleep
from datetime import datetime
from .parttable import DiskLayout
from .image import PartitionImage
from .diskdetect import get_disk_list, get_disk_details
from .sqfs import SquashWrapper
from backupset import BackupSet, Partition
from pprint import pprint


class ProcessController:
    __metaclass__ = ABCMeta

    def __init__(self, disk, job_id, config):
        self.disk = disk
        self.job_id = job_id
        self.config = config
        self.backup_dir = constants.BACKUP_PATH + str(job_id) + '/'
        self._init_status()
        self.backupset = None
        self._thread = None
        self._disk_layout = DiskLayout.with_config(self.disk, self.backup_dir, config)

    def get_status(self):
        self._update_status()
        return self._status

    @abstractmethod
    def run(self):
        pass

    def _init_status(self):
        self._status = {
            'status': '',
            'path': '',
            'layout': '',
            'partitions': [],
            'start_time': '',
            'end_time': '',
            'operation': '',
        }

    def _update_status(self):
        if self._imager:
            self._status['partitions'] = self._imager.get_status()


class BackupController(ProcessController):
    def __init__(self, disk, job_id, config):
        ProcessController.__init__(self, disk, job_id, config)
        self._status['operation'] = 'Backup'
        self._create_backupset()
        self._imager = PartitionImage.with_config(self.disk, self.backup_dir,
                                                  self.backupset, config)

    def run(self):
        self.backup()

    def backup(self):
        self._thread = Thread(target=self._backup)
        self._thread.start()

    def _backup(self):
        try:
            self._create_backupset()
            self._create_backup_directory()
            self._status['status'] = constants.STATUS_RUNNING
            self._status['start_time'] = datetime.today().strftime(constants.DATE_FORMAT)
            self._status['path'] = self.backup_dir
            self._status['layout'] = self.backupset.disk_layout
            self._disk_layout.backup_layout()
            self._imager.backup()
            self._status['status'] = constants.STATUS_FINISHED
            self._complete_backupset()
        except Exception as e:
            self._status['status'] = constants.STATUS_ERROR
            self._status['error_msg'] = str(e)
        finally:
            self._status['end_time'] = datetime.today().strftime(constants.DATE_FORMAT)

    def _create_backupset(self):
        disk_details = get_disk_details(self.disk)
        self.backupset = BackupSet(self.job_id)
        self.backupset.disk_layout = self._disk_layout.detect_layout()
        self.backupset.partition_table = self.backup_dir + 'ptable.bak'
        self.backupset.boot_record = self.backup_dir + 'mbr.img'
        self.backupset.disk_size = disk_details['size']
        self.backupset.compressed = self.config['compress']
        for partition in disk_details['partitions']:
            partition_number = partition['name'][-1]
            self.backupset.partitions.append(Partition(partition_number,
                                             partition['fs'], partition['size']))

    def _complete_backupset(self):
        self.backupset.creation_date = datetime.today().strftime(constants.DATE_FORMAT)
        self.backupset.backup_size = sum(path.getsize(self.backup_dir + f) for f in listdir(self.backup_dir))
        with open(self.backup_dir + constants.BACKUPSET_FILE, 'w') as fd:
            fd.write(str(self.backupset.to_json()))

    def _create_backup_directory(self):
        if not path.exists(self.backup_dir):
            try:
                makedirs(self.backup_dir)
            except IOError as e:
                logging.error('Cannot create path for backup: ' +
                              self.backup_dir + '. Cause:' + str(e))
                raise e


class RestorationController(ProcessController):
    UNMOUNT_DELAY = 3000

    def __init__(self, disk, backup_id, config):
        ProcessController.__init__(self, disk, backup_id, config)
        self.backupset = self._load_backupset()
        self._status['operation'] = 'Restoration'
        self._imager = PartitionImage.with_config(self.disk, self.backup_dir,
                                                  self.backupset, config)

    def _load_backupset(self):
        backupset_path = self.backup_dir + constants.BACKUPSET_FILE
        if path.exists(backupset_path):
            with open(backupset_path) as fd:
                content = fd.read()
                return BackupSet.from_json(literal_eval(content))
        else:
            raise Exception('Could not open backup information.')

    def run(self):
        self.restore()

    def restore(self):
        self._thread = Thread(target=self._restore)
        self._thread.start()

    def _restore(self):
        if self.backupset.compressed:
            self._mount_sqfs()
        try:
            self._status['status'] = constants.STATUS_RUNNING
            self._status['start_time'] = datetime.today().strftime(constants.DATE_FORMAT)
            self._status['path'] = self.backup_dir
            self._status['layout'] = self.backupset.disk_layout
            self._disk_layout.restore_layout(self.backupset.disk_layout)
            self._imager.restore()
            self._status['status'] = constants.STATUS_FINISHED
        except Exception as e:
            self._status['status'] = constants.STATUS_ERROR
            self._status['error_msg'] = str(e)
            if self.backupset.compressed:
                self._imager._runner.kill()
        finally:
            self._status['end_time'] = datetime.today().strftime(constants.DATE_FORMAT)
            if self.backupset.compressed:
                self._umount_sqfs()

    def _mount_sqfs(self):
        self.squash_wrapper = SquashWrapper(self.backupset)
        self.squash_wrapper.mount()

    def _umount_sqfs(self):
        if self.squash_wrapper:
            if self.squash_wrapper.mounted:
                self.squash_wrapper.umount()
