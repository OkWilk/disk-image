"""Author: Oktawiusz Wilk
Date: 30/11/2015
"""
import logging
from abc import ABCMeta, abstractmethod
from threading import Thread
from os import path, makedirs, listdir
from datetime import datetime
from .parttable import DiskLayout
from .image import PartitionImage
from .diskdetect import detect_disks
from backupset import BackupSet, Partition
from pprint import pprint


class ProcessController:
    __metaclass__ = ABCMeta

    DATE_FORMAT = '%d/%m/%Y %H:%M:%S'
    BACKUP_PATH = '/tmp/'
    STATUS_RUNNING = 'running'
    STATUS_ERROR = 'error'
    STATUS_FINISHED = 'finished'

    def __init__(self, disk, job_id, config):
        self.disk = disk
        self.job_id = job_id
        self.backup_dir = self.BACKUP_PATH + str(job_id) + '/'
        self._disk_layout = DiskLayout.with_config(self.disk, self.backup_dir, config)
        self._imager = PartitionImage.with_config(self.disk, self.backup_dir, config)
        self._init_status()
        self.backupset = None
        self._thread = None

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
            'partitions': '',
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

    def run(self):
        self.backup()

    def backup(self):
        self._thread = Thread(target=self._backup)
        self._thread.start()

    def _backup(self):
        try:
            self._create_backupset()
            self._create_backup_directory()
            self._status['status'] = self.STATUS_RUNNING
            self._status['start_time'] = datetime.today().strftime(self.DATE_FORMAT)
            self._status['path'] = self.backup_dir
            self._status['layout'] = self.backupset.disk_layout
            self._disk_layout.backup_layout()
            self._imager.backup()
            self._status['status'] = self.STATUS_FINISHED
            self._complete_backupset()
        except Exception as e:
            self._status['status'] = self.STATUS_ERROR
            self._status['error_msg'] = str(e)
        finally:
            self._status['end_time'] = datetime.today().strftime(self.DATE_FORMAT)

    def _create_backupset(self):
        disk_details = detect_disks()[self.disk]
        self.backupset = BackupSet(self.job_id)
        self.backupset.disk_layout = self._disk_layout.detect_layout()
        self.backupset.partition_table = self.backup_dir + 'ptable.bak'
        self.backupset.boot_record = self.backup_dir + 'mbr.img'
        self.backupset.disk_size = disk_details['size']
        for partition in disk_details['partitions']:
            partition_number = partition['name'][-1]
            self.backupset.partitions.append(Partition(partition_number,
                                             partition['fs'], partition['size']))

    def _complete_backupset(self):
        self.backupset.creation_date = datetime.today().strftime(self.DATE_FORMAT)
        self.backupset.backup_size = sum(path.getsize(self.backup_dir + f) for f in listdir(self.backup_dir))
        pprint(self.backupset.to_json())

    def _create_backup_directory(self):
        if not path.exists(self.backup_dir):
            try:
                makedirs(self.backup_dir)
            except IOError as e:
                logging.error('Cannot create path for backup: ' +
                              self.backup_dir + '. Cause:' + str(e))
                raise e


class RestorationController(ProcessController):
    def __init__(self, disk, backupset, config):
        ProcessController.__init__(self, disk, backupset.id, config)
        self.backupset = backupset
        self._status['operation'] = 'Restoration'

    def restore(self):
        self._thread = Thread(target=self._restore)
        self._thread.start()

    def _restore(self):
        try:
            self._status['status'] = self.STATUS_RUNNING
            self._status['start_time'] = datetime.today().strftime(self.DATE_FORMAT)
            self._status['path'] = self.backup_dir
            self._status['layout'] = 'MBR'
            self._disk_layout.restore_layout()
            self._imager.restore()
            self._status['status'] = self.STATUS_FINISHED
        except Exception as e:
            self._status['status'] = self.STATUS_ERROR
            self._status['error_msg'] = str(e)
        finally:
            self._status['end_time'] = datetime.today().strftime(self.DATE_FORMAT)
