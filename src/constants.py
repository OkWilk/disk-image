"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

DATE_FORMAT = '%d/%m/%Y %H:%M:%S'

STATUS_PENDING = 'pending'
STATUS_RUNNING = 'running'
STATUS_ERROR = 'error'
STATUS_FINISHED = 'finished'

# File Constants
DEVICE_PATH = '/dev/'
CONFIG_FILE = '/etc/diskimage/node/server.conf'
BACKUPSET_FILE = 'backupset.cfg'
PARTITION_TABLE_FILE = 'ptable.bak'
BOOT_RECORD_FILE = 'boot.img'
PARTITION_FILE_PREFIX = 'part'
PARTITION_FILE_SUFFIX = '.img'

# Interval Constants in seconds
REFRESH_DELAY = 5
METRIC_INTERVAL = 5
DISK_IO_INTERVAL = 1
BUSY_WAIT_INTERVAL = 0.01