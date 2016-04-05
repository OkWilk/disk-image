"""Author: Oktawiusz Wilk
Date: 30/11/2015
"""
import logging
from abc import ABCMeta, abstractmethod
from datetime import datetime
from logging import getLogger
from os import path, makedirs, listdir
from threading import Thread

from jinja2.nodes import Concat

import constants as constants
from core.backupset import Backupset
from core.diskdetect import DiskDetect
from core.image import PartitionImage
from core.nbdpool import NBDPool
from core.parttable import DiskLayout
from core.sqfs import SquashfsWrapper
from lib.exceptions import DiskImageException, ImageException
from services.config import ConfigHelper
from services.utils import delete_backup, delete_dir, create_dir


class BasicController:
    __metaclass__ = ABCMeta

    def __init__(self, backup_id):
        self._logger = getLogger(__name__)
        self.backup_id = backup_id
        self.backupset = None
        self._status = {
            'id': str(backup_id),
            'status': constants.STATUS_PENDING,
        }

    def get_status(self):
        return self._status

    def has_error_status(self):
        return self._status['status'] == constants.STATUS_ERROR

    def _set_error(self, msg):
        self._status['status'] = constants.STATUS_ERROR
        self._status['error_msg'] = str(msg)


class ProcessController(BasicController):
    __metaclass__ = ABCMeta

    def __init__(self, disk, backup_id, config):
        super(ProcessController, self).__init__(backup_id)
        self.disk = disk
        self.config = config
        self.backup_dir = ConfigHelper.config['Node']['Backup Path'] + str(backup_id) + '/'
        self._thread = None
        self._imager = None
        self._disk_layout = None
        self._status.update({
            'status': '',
            'path': '',
            'layout': '',
            'partitions': [],
            'start_time': '',
            'end_time': '',
            'operation': '',
        })

    def get_status(self):
        self._update_status()
        return self._status

    def kill(self):
        if self._imager:
            self._imager.kill()
        self._set_error("Job cancelled by the user.")

    @abstractmethod
    def run(self):
        pass

    def _update_status(self):
        if self._imager:
            self._status['partitions'] = self._imager.get_status()


class BackupController(ProcessController):
    def __init__(self, disk, backup_id, config):
        super(BackupController, self).__init__(disk, backup_id, config)
        try:
            self._disk_layout = DiskLayout.with_config(self.disk, self.backup_dir, config)
        except Exception as e:
            self._set_error(e)
        if self.config['overwrite']:
            self._remove_previous_backup()
        if not self.has_error_status():
            self._create_backupset()
            self._imager = PartitionImage.with_config(self.disk, self.backup_dir, self.backupset, config)

    def run(self):
        self._thread = Thread(target=self._backup)
        self._thread.start()

    def _remove_previous_backup(self):
        try:
            backupset = Backupset.load(self.backup_id)
            delete_backup(backupset)
        except DiskImageException as e:
            self._set_error(str(e))

    def _init_status(self):
        self._status['operation'] = 'Backup'
        self._status['status'] = constants.STATUS_RUNNING
        self._status['start_time'] = datetime.today().strftime(constants.DATE_FORMAT)
        self._status['path'] = self.backup_dir
        self._status['layout'] = self.backupset.disk_layout

    def _backup(self):
        if not self.has_error_status():
            try:
                self._init_status()
                self._create_backup_directory()
                self._disk_layout.backup_layout()
                self._imager.backup()
                self._status['status'] = constants.STATUS_FINISHED
            except Exception as e:
                self._set_error(e)
            finally:
                self._status['end_time'] = datetime.today().strftime(constants.DATE_FORMAT)
                self._complete_backupset()

    def _create_backup_directory(self):
        if not path.exists(self.backup_dir):
            try:
                makedirs(self.backup_dir)
            except IOError as e:
                logging.error('Cannot create path for backup: ' +
                              self.backup_dir + '. Cause:' + str(e))
                raise e

    def _create_backupset(self):
        disk_details = DiskDetect.get_disk_details(self.disk)
        self.backupset = Backupset(self.backup_id)
        self.backupset.disk_layout = self._disk_layout.get_layout()
        self.backupset.disk_size = disk_details['size']
        self.backupset.compressed = self.config['compress']
        self.backupset.add_partitions(disk_details['partitions'])
        self.backupset.save()

    def _complete_backupset(self):
        self.backupset.status = self._status['status']
        self.backupset.backup_size = sum(path.getsize(self.backup_dir + f) for f in listdir(self.backup_dir))
        self.backupset.save()


class RestorationController(ProcessController):

    def __init__(self, disk, backup_id, config):
        super(RestorationController, self).__init__(disk, backup_id, config)
        self.backupset = self._load_backupset()
        self._disk_layout = DiskLayout.with_config(self.disk, self.backup_dir, config, self.backupset.disk_layout)
        self._imager = PartitionImage.with_config(self.disk, self.backup_dir,
                                                  self.backupset, config)

    def _load_backupset(self):
        self.backupset = Backupset.load(self.backup_id)
        if self.backupset.node == ConfigHelper.config['Node']['Name']:
            return self.backupset
        else:
            raise Exception('This backup resides on another node, terminating.')

    def run(self):
        self._thread = Thread(target=self._restore)
        self._thread.start()

    def _init_status(self):
        self._status['status'] = constants.STATUS_RUNNING
        self._status['operation'] = 'Restoration'
        self._status['start_time'] = datetime.today().strftime(constants.DATE_FORMAT)
        self._status['path'] = self.backupset.backup_path
        self._status['layout'] = self.backupset.disk_layout

    def _restore(self):
        try:
            self._init_status()
            if self.backupset.compressed:
                self._mount_sqfs()
            self._disk_layout.restore_layout()
            self._imager.restore()
            self._status['status'] = constants.STATUS_FINISHED
        except Exception as e:
            self._set_error(e)
            if self.backupset.compressed:
                self._imager._runner.kill()
        finally:
            self._status['end_time'] = datetime.today().strftime(constants.DATE_FORMAT)
            if self.backupset.compressed:
                self._umount_sqfs()

    def _mount_sqfs(self):
        self.squash_wrapper = SquashfsWrapper(self.backupset)
        self.squash_wrapper.mount()

    def _umount_sqfs(self):
        if self.squash_wrapper:
            if self.squash_wrapper.mounted:
                self.squash_wrapper.umount()


class MountController(BasicController):
    NODE_POOL = NBDPool

    def __init__(self, backup_id):
        super(MountController, self).__init__(backup_id)
        self.nodes = []
        self.backupset = Backupset.load(backup_id)
        self.mount_path = ConfigHelper.config['Node']['Mount Path'] + self.backupset.id + '/'

    def mount(self):
        create_dir(self.mount_path)
        self._mount_partitions()
        self._status['status'] = constants.STATUS_RUNNING
        if not self._is_mounted_correctly():
            self._release_nodes()
            delete_dir(self.mount_path)

    def unmount(self):
        if self._can_unmount():
            self._release_nodes()
        delete_dir(self.mount_path)

    def _mount_partitions(self):
        for partition in self.backupset.partitions:
            node = self.NODE_POOL.acquire()
            image_path = self._get_image_path(partition)
            image_mount_path = self._get_image_mount_path(partition)
            create_dir(image_mount_path)
            node.mount(image_path, partition.file_system, image_mount_path)
            self.nodes.append(node)

    def _get_image_path(self, partition):
        return self.backupset.backup_path + constants.PARTITION_FILE_PREFIX + partition.id + \
                         constants.PARTITION_FILE_SUFFIX

    def _get_image_mount_path(self, partition):
        return self.mount_path + constants.PARTITION_FILE_PREFIX + partition.id + '/'

    def _is_mounted_correctly(self):
        error_detected = False
        for node in self.nodes:
            if node.error and not error_detected:
                error_detected = True
                self._set_error("Error detected duirng the mount operation on backup " + str(self.backupset.id))
        return not error_detected

    def _release_nodes(self):
        for node in self.nodes:
            node.unmount()
            self.NODE_POOL.release(node)
        del self.nodes[:]

    def _can_unmount(self): # TODO: implement code to check if mounted resource is busy
        return True
