import logging
from os import mkdir
from shutil import rmtree
from threading import Lock

from core.backupset import Backupset
from lib.exceptions import BackupOperationException, IllegalOperationException, DiskSpaceException
from .config import ConfigHelper
from .database import DB


def delete_backup(backupset):
    if backupset.deleted:
        if backupset.node == ConfigHelper.config['Node']['Name']:  # FIXME: This code will stop other nodes from deleting the backup files, it needs to be changed.
            _remove_backup_files(backupset)
        else:
            raise IllegalOperationException("The requested backup resides on a different node. " +
                                            "Please use node: " + backupset.node + " for this backup overwrite.")
    else:
        raise IllegalOperationException("A backup must be marked as ready for deletion before trying to overwrite it.")


def _remove_backup_files(backupset):
    try:
        rmtree(backupset.backup_path)
        backupset.mark_as_purged()
    except Exception as e:
        raise BackupOperationException('Cannot remove backup, cause: ' + str(e))


def create_dir(dir):
    try:
        mkdir(dir)
    except FileExistsError:
        pass


def delete_dir(dir):
    try:
        rmtree(dir)
    except:
        pass  # TODO: do something better here...


class _BackupRemover:
    MULTIPLIERS = {
        'kb': 1024,
        'mb': 1048576,
        'gb': 1073741824
    }

    def __init__(self):
        self._lock = Lock()
        self._logger = logging.getLogger(__name__)

    def handle_space_error(self, error_message):
        details = self._parse_error_to_size(error_message)
        self._make_space(details['space_required'])

    def _parse_error_to_size(self, error):
        error_message = str(error)
        sizes = [self._string_to_bytes(x.strip()) for x in error_message[error_message.rindex(':') + 1:].strip().split('<')]
        details = {
            'backup_size': sizes[1],
            'current_space': sizes[0],
            'space_required': sizes[1] - sizes[0]
        }
        return details

    def _string_to_bytes(self, space):
        space = space.strip()
        for unit in self.MULTIPLIERS:
            if unit in space:
                space = int(space.split(unit)[0])
                return space * self.MULTIPLIERS[unit]
        self._logger.warning('Cannot calculate space in bytes for the received input. Input: ' + str(space))
        raise ValueError('Invalid string format or unknown unit received.')

    def _make_space(self, space_required):
        self._logger.debug('creating purge list')
        with self._lock:
            purge_list = []
            remaining_space_required = space_required
            for backup in DB.get_backups_for_purging():
                if remaining_space_required > 0:
                    print(str(backup['backup_size']))
                    remaining_space_required -= int(backup['backup_size'])
                    purge_list.append(backup)
            if remaining_space_required > 0:
                raise Exception('Unable to free up required disk space for backup. Remaining disk space required would be: '
                                         + str(remaining_space_required))
            for backup in purge_list:
                self._logger.debug(str(backup['id']) + ': ' + str(backup['backup_size']))
                self._purge_backup(backup)
            self._logger.debug('Remaining_space_required: ' + str(remaining_space_required))

    def _purge_backup(self, backup):
        _remove_backup_files(Backupset.load(backup['id']))


BackupRemover = _BackupRemover()