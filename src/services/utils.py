"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

import logging
from os import mkdir
from shutil import rmtree
from threading import Lock

from humanize import naturalsize

from core.backupset import Backupset
from lib.exceptions import BackupOperationException, IllegalOperationException
from .config import ConfigHelper
from .database import DB


def delete_backup(backupset):
    """
    Physically removes the backup files from the disk, for the backup described by the backupset.
    If the backup resides on another node an exception will be raised.
    :param backupset: an object of the Backupset class that describes the
    :return: None
    """
    if backupset.deleted:
        if backupset.node == ConfigHelper.config['node']['name']:
            _remove_backup_files(backupset)
        else:
            raise IllegalOperationException("The requested backup resides on a different node. " +
                                            "Please use node: " + backupset.node + " for this backup overwrite.")
    else:
        raise IllegalOperationException("A backup must be marked as ready for deletion before overwriting it.")


def _remove_backup_files(backupset):
    try:
        rmtree(backupset.backup_path)
        backupset.mark_as_purged()
    except Exception as e:
        raise BackupOperationException('Cannot remove backup, cause: ' + str(e))


def create_dir(dir):
    """
    Creates a directory if it doesn't exist.
    :param dir: directory name to be created.
    :return: None
    """
    try:
        mkdir(dir)
    except FileExistsError:
        pass


def delete_dir(dir):
    """
    Removes a directory if it exists.
    :param dir: directory name to be deleted.
    :return: None
    """
    try:
        rmtree(dir)
    except:
        pass


class _BackupRemover:
    """
    This class manages purging of the backups when DiskSpaceError is raised.
    It will parse the partclone output containing the information about the remaining space
    necessary and try to purge backups in the order from the oldest to the newest.
    """

    MULTIPLIERS = {
        'kb': 1024,
        'mb': 1048576,
        'gb': 1073741824
    }

    def __init__(self):
        self._lock = Lock()
        self._logger = logging.getLogger(__name__)

    def handle_space_error(self, error_message):
        """
        Starts the procedure to clear the disk space required if possible.
        :param error_message: the exact error message from the partclone containing the
            information about the remaining disk space required to store backup.
        :return: None
        """
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
                    remaining_space_required -= int(backup['backup_size'])
                    purge_list.append(backup)
            if remaining_space_required > 0:
                raise Exception('Unable to free up required disk space for backup. Remaining disk space required would be: '
                                         + str(naturalsize(remaining_space_required)))
            for backup in purge_list:
                self._logger.debug(str(backup['id']) + ': ' + str(backup['backup_size']))
                self._purge_backup(backup)
            self._logger.debug('Remaining_space_required: ' + str(naturalsize(remaining_space_required)))

    def _purge_backup(self, backup):
        _remove_backup_files(Backupset.load(backup['id']))


# Export as singleton.
BackupRemover = _BackupRemover()