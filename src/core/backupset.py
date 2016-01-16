import constants as constants
from services.config import ConfigHelper
from services.database import DB


class BackupSet:

    def __init__(self, backup_id):
        self.id = backup_id
        self.node = ConfigHelper.config['Node']['Name']
        self.backup_path = ConfigHelper.config['Node']['Backup Path'] + self.id + '/'
        self.disk_layout = ""
        self.status = constants.STATUS_RUNNING
        self.deleted = False
        self.compressed = False
        self.backup_size = 0
        self.disk_size = 0
        self.deletion_date = ""
        self.creation_date = ""
        self.partitions = []

    @classmethod
    def retrieve(cls, backup_id):
        data = DB.get_backup(backup_id)
        if data:
            backupset = cls.from_json(data)
            if backupset.node == ConfigHelper.config['Node']['Name']:
                return backupset
            else:
                raise Exception('This backup resides on another node, terminating.')
        else:
            raise Exception('Could not retrieve backup information.')

    @classmethod
    def from_json(cls, json):
        backupset = cls(json.get('id'))
        backupset.node = json.get('node', backupset.node)
        backupset.backup_path = json.get('backup_path', backupset.backup_path)
        backupset.disk_layout = json.get('disk_layout')
        backupset.status = json.get('status')
        backupset.deleted = json.get('deleted')
        backupset.compressed = json.get('compressed')
        backupset.backup_size = json.get('backup_size')
        backupset.disk_size = json.get('disk_size')
        backupset.deletion_date = json.get('deletion_date')
        backupset.creation_date = json.get('creation_date')
        for partition in json.get('partitions'):
            backupset.partitions.append(Partition.from_json(partition))
        return backupset

    def to_json(self):
        json = {
            'id': self.id,
            'node': self.node,
            'backup_path': self.backup_path,
            'disk_layout': self.disk_layout,
            'status': self.status,
            'deleted': self.deleted,
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
