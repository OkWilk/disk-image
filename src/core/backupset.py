import constants as constants
from services.config import ConfigHelper
from services.database import DB
from datetime import datetime

class BackupSet:

    def __init__(self, backup_id):
        self.id = backup_id
        self.node = ConfigHelper.config['Node']['Name']
        self.backup_path = ConfigHelper.config['Node']['Backup Path'] + self.id + '/'
        self.disk_layout = ""
        self.status = constants.STATUS_RUNNING
        self.deleted = False
        self.purged = False
        self.compressed = False
        self.backup_size = 0
        self.disk_size = 0
        self.deletion_date = ""
        self.creation_date = datetime.today()
        self.partitions = []

    @classmethod
    def load(cls, backup_id):
        data = DB.get_backup(backup_id)
        if data:
            backupset = cls._from_json(data)
            return backupset
        raise Exception('Could not retrieve backup information.')

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
        for partition in json.get('partitions'):
            backupset.partitions.append(Partition.from_json(partition))
        return backupset

    def save(self):
        DB.upsert_backup(self.id, self.to_json())

    def add_partitions(self, partitions):
        for partition in partitions:
            partition_number = partition['name'][-1]
            self.partitions.append(Partition(partition_number, partition['fs'], partition['size']))

    def to_json(self):
        json = {
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
            'partitions': [],
        }
        for partition in self.partitions:
            json['partitions'].append(partition.to_json())
        return json


class Partition:
    def __init__(self, partition_id, file_system, size):
        self.id = partition_id
        self.file_system = file_system
        self.size = size

    @classmethod
    def from_json(cls, json):
        return cls(json.get('partition'), json.get('fs'), json.get('size'))

    def to_json(self):
        return {
            'partition': self.id,
            'fs': self.file_system,
            'size': self.size,
        }
