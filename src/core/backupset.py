"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from datetime import datetime

import constants as constants
from lib.exceptions import BackupsetException, IllegalOperationException
from services.config import ConfigHelper
from services.database import DB


class Backupset:
    """
    This class provides a structure required to represent information regarding
    the physical backups. It is used to provide an encapsulation of the data stored
    in the database and to provide a consistent way of operating on the backup records.
    """
    def __init__(self, backup_id):
        self.id = backup_id
        self.node = ConfigHelper.config['node']['name']
        self.backup_path = ConfigHelper.config['node']['backup_path'] + self.id + '/'
        self.disk_layout = ""
        self.status = constants.STATUS_RUNNING
        self.deleted = False
        self.purged = False
        self.compressed = False
        self.backup_size = 0
        self.disk_size = 0
        self.deletion_date = ''
        self.creation_date = datetime.today()
        self.purge_date = ''
        self.partitions = []

    @classmethod
    def load(cls, backup_id):
        """
        Loads the backup information from the datastore used and returns a ready to use object.
        :param backup_id: the string identifier of the backup data to be loaded.
        :return: a fully initialised Backupset object with information loaded from the datastore.
        """
        data = DB.get_backup(backup_id)
        if data:
            backupset = cls._from_json(data)
            return backupset
        raise BackupsetException('Could not retrieve backup information.')

    @classmethod
    def _from_json(cls, json):
        backupset = cls(json.get('id'))
        backupset.node = json.get('node', backupset.node)
        backupset.backup_path = json.get('backup_path', backupset.backup_path)
        backupset.disk_layout = json.get('disk_layout')
        backupset.status = json.get('status')
        backupset.deleted = json.get('deleted')
        backupset.purged = json.get('purged')
        backupset.compressed = json.get('compressed')
        backupset.backup_size = json.get('backup_size')
        backupset.disk_size = json.get('disk_size')
        backupset.deletion_date = json.get('deletion_date')
        backupset.creation_date = json.get('creation_date')
        backupset.purge_date = json.get('purge_date')
        for partition in json.get('partitions'):
            backupset.partitions.append(Partition.from_json(partition))
        return backupset

    def mark_as_purged(self):
        """
        Marks backup as a physically removed from the hard disk drive.
        :return: None
        :exception: IllegalOperationException will be raised if the backup to be purged was not
            marked for deletion.
        """
        if self.deleted:
            self.purged = True
            self.purge_date = datetime.today()
            self.save()
        else:
            raise IllegalOperationException('The backup to be purged, was not marked for deletion yet.')

    def save(self):
        """
        Updates the information regarding backup in the datastore. If the backup did not exist
        prior this call it will be automatically created.
        :return: None
        """
        DB.upsert_backup(self.id, self.to_dict())

    def add_partitions(self, partitions):
        """
        Adds partition information to the backupset.
        :param partitions: dictionary with details regarding the partition to be added,
            the expected parameters are:
                name - partition name (e.g. sda1)
                fs - the file system used by the partition
                size - the partition size in bytes
        :return: None
        """
        for partition in partitions:
            partition_number = partition['name'][-1]
            self.partitions.append(Partition(partition_number, partition['fs'], partition['size']))

    def to_dict(self):
        """
        Creates a dictionary representation of the data stored by the Backupset object.
        :return: dictionary object with keys matching all public members of the Backupset class.
        """
        data = {
            'id': self.id,
            'node': self.node,
            'backup_path': self.backup_path,
            'disk_layout': self.disk_layout,
            'status': self.status,
            'deleted': self.deleted,
            'purged': self.purged,
            'compressed': self.compressed,
            'backup_size': self.backup_size,
            'disk_size': self.disk_size,
            'deletion_date': self.deletion_date,
            'creation_date': self.creation_date,
            'purge_date': self.purge_date,
            'partitions': [],
        }
        for partition in self.partitions:
            data['partitions'].append(partition.to_dict())
        return data


class Partition:
    """
    This class provides a structure for representing the partition information in backupsets.
    """
    def __init__(self, partition_id, file_system, size):
        self.id = partition_id
        self.file_system = file_system
        self.size = size

    @classmethod
    def from_json(cls, json):
        """
        Loads data stored in the JSON format and returns a ready partition object.
        :param json: Data required to build object.
        :return: a fully initialised Partition object.
        """
        return cls(json.get('partition'), json.get('fs'), json.get('size'))

    def to_dict(self):
        """
        Creates a dictionary representation of the data stored by the Partition object.
        :return: dictionary object with keys matching all public members of the Partition class.
        """
        return {
            'partition': self.id,
            'fs': self.file_system,
            'size': self.size,
        }
