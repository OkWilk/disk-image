"""Author: Oktawiusz Wilk
Date: 30/11/2015
"""
import logging
from abc import ABCMeta, abstractmethod
from datetime import datetime
from os import path, makedirs, listdir, mkdir
from shutil import rmtree
from threading import Thread

import constants as constants
from core.backupset import BackupSet, Partition
from core.diskdetect import DiskDetect
from core.image import PartitionImage
from core.nbdpool import NBDPool
from core.parttable import DiskLayout
from core.sqfs import SquashWrapper
from services.config import ConfigHelper
from services.database import DB, ItemExistsException

#TODO: Refactor to add superclass for mount and process controller

class ProcessController:
    __metaclass__ = ABCMeta

    def __init__(self, disk, job_id, config):
        self.disk = disk
        self.job_id = job_id
        self.config = config
        self.backup_dir = constants.BACKUP_PATH + str(job_id) + '/'
        self.backupset = None
        self._thread = None
        self._imager = None
        self._disk_layout = DiskLayout.with_config(self.disk, self.backup_dir, config)
        self._status = {
            'status': '',
            'path': '',
            'layout': '',
            'partitions': [],
            'start_time': '',
            'end_time': '',
            'operation': '',
        }

    def get_status(self):
        self._update_status()
        return self._status

    @abstractmethod
    def run(self):
        pass

    def _set_error(self, msg):
        self._status['status'] = constants.STATUS_ERROR
        self._status['error_msg'] = str(msg)

    def _update_status(self):
        if self._imager:
            self._status['partitions'] = self._imager.get_status()


class BackupController(ProcessController):
    def __init__(self, disk, job_id, config):
        ProcessController.__init__(self, disk, job_id, config)
        if self.config['overwrite']:
            self._remove_previous_backup()
        if self._status['status'] != constants.STATUS_ERROR:
            try:
                self._create_backupset()
                self._imager = PartitionImage.with_config(self.disk, self.backup_dir, self.backupset, config)
            except ItemExistsException as e:
                self._set_error(e)

    def run(self):
        self._thread = Thread(target=self._backup)
        self._thread.start()

    def _remove_previous_backup(self):
        try:
            backupset = BackupSet.retrieve(self.job_id)
            if backupset:
                if backupset.deleted:
                    DB.remove_backup(self.job_id)
                elif backupset.node == ConfigHelper.config['Node']['Name']:
                    try:
                        rmtree(backupset.backup_path)
                        DB.remove_backup(self.job_id)
                    except Exception as e:
                        self._set_error('Cannot remove backup, cause: ' + str(e))
                else:
                    self._set_error("The requested backup resides on a different node. " +
                                    "Please use node: " + backupset.node + " for this backup overwrite.")
        except:
            self._set_error("Cannot retrieve backup information, try again later.")

    def _init_status(self):
        self._status['operation'] = 'Backup'
        self._status['status'] = constants.STATUS_RUNNING
        self._status['start_time'] = datetime.today().strftime(constants.DATE_FORMAT)
        self._status['path'] = self.backup_dir
        self._status['layout'] = self.backupset.disk_layout

    def _backup(self):
        if self._status['status'] != constants.STATUS_ERROR:
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
        self.backupset = BackupSet(self.job_id)
        self.backupset.creation_date = datetime.today().strftime(constants.DATE_FORMAT)
        self.backupset.disk_layout = self._disk_layout.detect_layout()
        self.backupset.disk_size = disk_details['size']
        self.backupset.compressed = self.config['compress']
        self._add_partitions_to_backupset(disk_details['partitions'])
        DB.insert_backup(self.backupset.id, self.backupset.to_json())

    def _add_partitions_to_backupset(self, partitions):
        for partition in partitions:
            partition_number = partition['name'][-1]
            self.backupset.partitions.append(Partition(partition_number, partition['fs'], partition['size']))

    def _complete_backupset(self):
        self.backupset.status = self._status['status']
        self.backupset.creation_date = datetime.today().strftime(constants.DATE_FORMAT)
        self.backupset.backup_size = sum(path.getsize(self.backup_dir + f) for f in listdir(self.backup_dir))
        DB.update_backup(self.backupset.id, self.backupset.to_json())


class RestorationController(ProcessController):
    UNMOUNT_DELAY = 3000

    def __init__(self, disk, backup_id, config):
        ProcessController.__init__(self, disk, backup_id, config)
        self.backupset = self._load_backupset()
        self._imager = PartitionImage.with_config(self.disk, self.backup_dir,
                                                  self.backupset, config)

    def _load_backupset(self):  # TODO: replace with BACKUPSET.retrieve
        data = DB.get_backup(self.job_id)
        if data:
            backupset = BackupSet.from_json(data)
            if backupset.node == ConfigHelper.config['Node']['Name']:
                return backupset
            else:
                raise Exception('This backup resides on another node, terminating.')
        else:
            raise Exception('Could not retrieve backup information.')

    def run(self):
        self._thread = Thread(target=self._restore)
        self._thread.start()

    def _init_status(self):
        self._status['operation'] = 'Restoration'
        self._status['status'] = constants.STATUS_RUNNING
        self._status['start_time'] = datetime.today().strftime(constants.DATE_FORMAT)
        self._status['path'] = self.backupset.backup_path
        self._status['layout'] = self.backupset.disk_layout

    def _restore(self):
        try:
            self._init_status()
            if self.backupset.compressed:
                self._mount_sqfs()
            self._disk_layout.restore_layout(self.backupset.disk_layout)
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
        self.squash_wrapper = SquashWrapper(self.backupset)
        self.squash_wrapper.mount()

    def _umount_sqfs(self):
        if self.squash_wrapper:
            if self.squash_wrapper.mounted:
                self.squash_wrapper.umount()


class MountController:
    NODE_POOL = NBDPool

    def __init__(self, backup_id):
        self.nodes = []
        self.backupset = BackupSet.retrieve(backup_id)
        self.mount_path = constants.MOUNT_PATH + self.backupset.id + '/'
        self._status = {}
        self._init_status()

    def get_status(self):
        return self._status

    def is_error_status(self):
        return self._status['status'] == constants.STATUS_ERROR

    def _init_status(self):
        self._status = {
            'id': self.backupset.id,
            'status': constants.STATUS_PENDING,
        }

    def _set_error(self, msg):
        self._status['status'] = constants.STATUS_ERROR
        self._status['error_msg'] = str(msg)

    def mount(self):
        if self._can_mount():
            self._create_dir(self.mount_path)
            self._status['status'] = constants.STATUS_PENDING
            self._mount_partitions()
            self._status['status'] = constants.STATUS_RUNNING
        if not self._is_mounted_correctly():
            self._release_nodes()
            self._delete_dir(self.mount_path)

    def unmount(self):
        if self._can_unmount():
            self._release_nodes()
        self._delete_dir(self.mount_path)

    def _can_mount(self):
        return True

    def _mount_partitions(self):
        for partition in self.backupset.partitions:
            node = self.NODE_POOL.acquire()
            image_path = self._get_image_path(partition)
            image_mount_path = self._get_image_mount_path(partition)
            self._create_dir(image_mount_path)
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

    def _create_dir(self, dir):
        try:
            mkdir(dir)
        except FileExistsError:
            pass

    def _delete_dir(self, dir):
        try:
            rmtree(dir)
        except:
            pass  # TODO: do something better here...

    def _release_nodes(self):
        for node in self.nodes:
            node.unmount()
            self.NODE_POOL.release(node)
        del self.nodes[:]

    def _can_unmount(self):
        return True
