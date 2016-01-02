class BackupSet:
    def __init__(self, backup_id):
        self.id = backup_id
        self.disk_layout = ""
        self.boot_record = ""
        self.partition_table = ""
        self.compressed = False
        self.backup_size = 0
        self.disk_size = 0
        self.creation_date = 0
        self.partitions = []

    @classmethod
    def from_json(cls, json):
        backupset = cls(json['id'])
        backupset.disk_layout = json['disk_layout']
        backupset.boot_record = json['boot_record']
        backupset.partition_table = json['partition_table']
        backupset.backup_size = json['backup_size']
        backupset.disk_size = json['disk_size']
        backupset.creation_date = json['creation_date']
        backupset.compressed = json['compressed']
        for partition in json['partitions']:
            backupset.partitions.append(Partition.from_json(partition))
        return backupset

    def to_json(self):
        json = {
            'id': self.id,
            'disk_layout': self.disk_layout,
            'boot_record': self.boot_record,
            'partition_table': self.partition_table,
            'backup_size': self.backup_size,
            'disk_size': self.disk_size,
            'creation_date': self.creation_date,
            'compressed': self.compressed,
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
        return cls(json['partition'], json['fs'], json['size'])

    def to_json(self):
        return {
            'partition': self.id,
            'fs': self.file_system,
            'size': self.size,
        }
