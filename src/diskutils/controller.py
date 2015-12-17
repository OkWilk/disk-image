"""Author: Oktawiusz Wilk
Date: 30/11/2015
"""
import logging
from threading import Thread
from os import path, makedirs
from datetime import datetime
from .parttable import DiskLayout
from .image import PartitionImage


class ProcessController:
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
        self._thread = None

    def get_status(self):
        self._update_status()
        return self._status

    def backup(self):
        self._thread = Thread(target=self._backup)
        self._thread.start()

    def _backup(self):
        try:
            DATE_FORMAT = '%d/%m/%Y %H:%M:%S'
            self._status['status'] = self.STATUS_RUNNING
            self._status['start_time'] = datetime.today().strftime(DATE_FORMAT)
            self._create_backup_directory()
            self._status['path'] = self.backup_dir
            self._status['layout'] = self._disk_layout.detect_layout()
            self._disk_layout.backup_layout()
            self._imager.backup()
            self._status['status'] = self.STATUS_FINISHED
        except Exception as e:
            self._status['status'] = self.STATUS_ERROR
            self._status['error_msg'] = str(e)
        finally:
            self._status['end_time'] = datetime.today().strftime(DATE_FORMAT)

    def _create_backup_directory(self):
        if not path.exists(self.backup_dir):
            try:
                makedirs(self.backup_dir)
            except IOError as e:
                logging.error('Cannot create path for backup: ' +
                              self.backup_dir + '. Cause:' + str(e))
                raise e

    def _init_status(self):
        self._status = {
            'status': '',
            'path': '',
            'layout': '',
            'partitions': '',
            'start_time': '',
            'end_time': '',
        }

    def _update_status(self):
        if self._imager:
            self._status['partitions'] = self._imager.get_status()
