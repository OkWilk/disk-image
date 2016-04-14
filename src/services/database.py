"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from abc import ABCMeta, abstractclassmethod
from threading import RLock

from pymongo import ASCENDING

import constants
from .config import ConfigHelper
from .mdbconnector import MongoConnector, to_list


class Database:
    """
    This class provides a public interface for database procedures, it can be extended to add
    later support for databases other than MongoDB.
    """
    __metaclass__ = ABCMeta

    def __init__(self, config):
        self._lock = RLock()
        self.config = config

    @abstractclassmethod
    def upsert_backup(self, backup_id, data):
        """
        Modifies the existing backup or creates a new one if required.
        :param backup_id: string identifier of the backup
        :param data: JSON object that should be written to the database under the backup_id
        :return: None
        """
        pass

    @abstractclassmethod
    def get_backup(self, backup_id):
        """
        Retrieves a specific instance of the backup from the database.
        :param backup_id: string identifier of the backup to be retrieved.
        :return: dictrionary containing the backup information.
        """
        pass

    @abstractclassmethod
    def remove_backup(self, backup_id):
        """
        Physically removes a specific backup from the database.
        :param backup_id: string identifier of the backup to be removed
        :return: None
        """
        pass

    @abstractclassmethod
    def get_backups_for_purging(self):
        """
        Retrieves a list of backups for the specific imaging node which can be purged.
        :return: list of backups to be purged sorted by the creation date.
        """
        pass

    @abstractclassmethod
    def remove_zombie_backups(self):
        """
        Removes "running" backups after Node restarts and crashes.
        :return: None
        """
        pass


class MongoDB(Database):
    """ The specific implementation of the Database interface for the MongoDB """

    def upsert_backup(self, backup_id, data):
        with self._lock:
            with MongoConnector(self.config) as db:
                db.backup.update_one({"id": backup_id}, {'$set': data}, True)

    def get_backup(self, backup_id):
        with self._lock:
            with MongoConnector(self.config) as db:
                return db.backup.find_one({'id': backup_id})

    def remove_backup(self, backup_id):
        with self._lock:
            with MongoConnector(self.config) as db:
                db.backup.remove({'id': backup_id})

    def get_backups_for_purging(self):
        with self._lock:
            with MongoConnector(self.config) as db:
                return to_list(db.backup.find({'node': ConfigHelper.config['node']['name'],
                                               'deleted': True,
                                               'purged': False}).sort('creation_date', ASCENDING))

    def remove_zombie_backups(self):
        with self._lock:
            with MongoConnector(self.config) as db:
                db.backup.update({'node': ConfigHelper.config['node']['name'],
                                  'status': constants.STATUS_RUNNING},
                                 {'$set': {'status': constants.STATUS_ERROR}})

# Export a ready Database client as a singleton.
DB = MongoDB(ConfigHelper.config['database'])