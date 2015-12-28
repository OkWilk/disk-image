class BackupSet:
    def __init__(self, backup_id):
        self.id = backup_id
        self.disk_layout = ""
        self.boot_record = ""
        self.partition_table = ""
        self.backup_size = 0
        self.disk_size = 0
        self.creation_date = 0
        self.partitions = []

    @classmethod
    def from_json(cls, json):
        backup_set = cls(json['id'])
        backup_set.disk_layout = json['disk_layout']
        backup_set.boot_record = json['boot_record']
        backup_set.partition_table = json['partition_table']
        backup_set.backup_size = json['backup_size']
        backup_set.disk_size = json['disk_size']
        backup_set.creation_date = json['creation_date']
        for partition in json['partitions']:
            backup_set.partitions.append(Partition.from_json(partition))

    def to_json(self):
        json = {
            'id': self.id,
            'disk_layout': self.disk_layout,
            'boot_record': self.boot_record,
            'partition_table': self.partition_table,
            'backup_size': self.backup_size,
            'disk_size': self.disk_size,
            'creation_date': self.creation_date,
            'partitions': [],
        }
        for partition in self.partitions:
            json['partitions'].append(partition.to_json())
        return json


class Partition:
    def __init__(self, partition_number, file_system, size):
        self.partition_number = partition_number
        self.file_system = file_system
        self.size = size

    @classmethod
    def from_json(cls, json):
        return cls(json['partition_number'], json['file_system'], json['size'])

    def to_json(self):
        return {
            'partition': self.partition_number,
            'fs': self.file_system,
            'size': self.size,
        }
