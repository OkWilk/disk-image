from threading import RLock
from abc import ABCMeta, abstractclassmethod

from pymongo import ASCENDING

from .config import ConfigHelper
from .mdbconnector import MongoConnector, to_list


class Database:  # TODO: mark as abstract class
    __metaclass__ = ABCMeta

    def __init__(self, config):
        self._lock = RLock()
        self.config = config

    @abstractclassmethod
    def upsert_backup(self, backup_id, data):
        pass

    @abstractclassmethod
    def get_backup(self, backup_id):
        pass

    @abstractclassmethod
    def remove_backup(self, backup_id):
        pass

    @abstractclassmethod
    def get_backups_for_purging(self):
        pass


class MongoDB(Database):

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
                return to_list(db.backup.find({'node': ConfigHelper.config['Node']['Name'],
                                               'deleted': True,
                                               'purged': False}).sort('creation_date', ASCENDING))


DB = MongoDB(ConfigHelper.config['Database'])