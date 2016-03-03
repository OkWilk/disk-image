from threading import RLock

from pymongo import ASCENDING

from .config import ConfigHelper
from .mdbconnector import MongoConnector, to_list


class Database:  # TODO: mark as abstract class
    def __init__(self, config):
        self._lock = RLock()
        self.config = config

    def upsert_backup(self, backup_id, data):
        pass

    def get_backup(self, backup_id):
        pass

    def remove_backup(self, backup_id):
        pass

    def purge_backup(self, backup_id):
        pass

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

    def purge_backup(self, backup_id):
        with self._lock:
            with MongoConnector(self.config) as db:
                db.backup.update_one({'id': backup_id}, {'$set': {'purged': True}})

    def get_backups_for_purging(self):
        with self._lock:
            with MongoConnector(self.config) as db:
                return to_list(db.backup.find({'node': ConfigHelper.config['Node']['Name'],
                                               'deleted': True,
                                               'purged': False}).sort('creation_date', ASCENDING))


DB = MongoDB(ConfigHelper.config['Database'])