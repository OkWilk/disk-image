from .mdbconnector import MongoConnector
from .config import ConfigHelper
from threading import RLock


class Database:  # TODO: mark as abstract class
    def __init__(self, config):
        self._lock = RLock()
        self.config = config

    def insert_backup(self, backup_id, data):
        pass

    def update_backup(self, backup_id, data):
        pass

    def get_backup(self, backup_id):
        pass

    def remove_backup(self, backup_id):
        pass


class MongoDB(Database):

    def insert_backup(self, backup_id, data):
        with self._lock:
            with MongoConnector(self.config) as db:
                if db.backup.find_one({'id': backup_id}):
                    raise ItemExistsException("A backup with id: '" + backup_id + "', already exists in the database.")
                db.backup.insert(data)

    def update_backup(self, backup_id, data):
        with self._lock:
            with MongoConnector(self.config) as db:
                result = db.backup.update({'id': backup_id}, {'$set': data}, True)

    def get_backup(self, backup_id):
        with self._lock:
            with MongoConnector(self.config) as db:
                return db.backup.find_one({'id': backup_id})

    def remove_backup(self, backup_id):
        with self._lock:
            with MongoConnector(self.config) as db:
                db.backup.remove({'id': backup_id})


class ItemExistsException(Exception):
    pass


DB = MongoDB(ConfigHelper.config['Database'])