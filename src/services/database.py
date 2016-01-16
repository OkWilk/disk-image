from .mdbconnector import MongoConnector
from .config import ConfigHelper
from threading import Lock


class Database:  # TODO: mark as abstract class
    def __init__(self, config):
        self.lock = Lock()
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
        self.lock.acquire()
        with MongoConnector(self.config) as db:
            if db.backup.find_one({'id': backup_id}):
                self.lock.release()
                raise ItemExistsException("A backup with id: '" + backup_id + "', already exists in the database.")
            db.backup.insert(data)
        self.lock.release()

    def update_backup(self, backup_id, data):
        self.lock.acquire()
        with MongoConnector(self.config) as db:
            result = db.backup.update({'id': backup_id}, {'$set': data})
            if result['n']:
                self.insert_backup(backup_id, data)
        self.lock.release()

    def get_backup(self, backup_id):
        with MongoConnector(self.config) as db:
            return db.backup.find_one({'id': backup_id})

    def remove_backup(self, backup_id):
        with MongoConnector(self.config) as db:
            db.backup.remove({'id': backup_id})


class ItemExistsException(Exception):
    pass


DB = MongoDB(ConfigHelper.config['Database'])