
DATE_FORMAT = '%d/%m/%Y %H:%M:%S'

STATUS_PENDING = 'pending'
STATUS_RUNNING = 'running'
STATUS_ERROR = 'error'
STATUS_FINISHED = 'finished'

# File Constants
BACKUP_DISK = 'sda'
DEVICE_PATH = '/dev/'
CONFIG_FILE = '/etc/diskimage/node/server.conf'
BACKUP_PATH = '/backup/'
MOUNT_PATH = BACKUP_PATH + 'mnt/'
BACKUPSET_FILE = 'backupset.cfg'
PARTITION_TABLE_FILE = 'ptable.bak'
BOOT_RECORD_FILE = 'boot.img'
PARTITION_FILE_PREFIX = 'part'
PARTITION_FILE_SUFFIX = '.img'

# Interval Constants in seconds
METRIC_INTERVAL = 5
BUSY_WAIT_INTERVAL = 0.01