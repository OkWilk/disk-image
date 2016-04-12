"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from flask import request
from flask_restful import Resource

import constants
from core.controller import MountController


class Mount(Resource):
    """ Defines the Web API for mounting, unmounting and retrieving information about mounted
    backups on the Imaging Node. """
    _mounts = {}

    def get(self, backup_id=None):
        """
        Provides information about backups or a single backup mounted on the node.
        :param backup_id: string defining the name of the mounted backup to be retrieved.
        :return: a list of mounted backups with statuses or a single backup with status if
            backup_id is present.
        """
        if backup_id:
            return self._get_mount_details(backup_id)
        else:
            return self._get_mount_list()

    def _get_mount_details(self, backup_id):
        for id in self._mounts.keys():
            if id == backup_id:
                return self._mounts[id]['controller'].get_status(), 200
        return 'Requested backup is not mounted on this node.', 404

    def _get_mount_list(self):
        payload = []
        for mount in self._mounts.values():
            payload.append(mount['controller'].get_status())
        return payload, 200

    def post(self):
        """
        Facilitates mounting of existing backups by sending HTTP POST request with JSON body.
        The JSON is expected to provide a backup_id for the backup to be mounted.
        :return: OK with 200 status code if backup was mounted properly,
            Error message with an appropriate HTTP status if backup cannot be mounted.
        """
        data = request.get_json(force=True)
        if 'backup_id' in data:
            if data['backup_id'] not in self._mounts.keys():
                return self._mount_backup(data['backup_id'])
            else:
                return 'The requested backup is already mounted.', 400
        else:
            return 'Invalid request format, the required backup_id field was not provided.', 400

    def _mount_backup(self, backup_id):
        try:
            controller = MountController(backup_id)
            controller.mount()
            if controller.get_status()['status'] != constants.STATUS_ERROR:
                self._mounts[backup_id] = {'controller': controller}
                return 'OK', 200
            else:
                return controller.get_status()['error_msg'], 500
        except Exception as e:
            return "Cannot mount backup '" + str(backup_id) + "', Cause: " + str(e), 400

    def delete(self, backup_id):
        """
        Unmounts previously mounted backup with the provided backup_id.
        :param backup_id: string identifier of the backup to be unmounted.
        :return: OK with 200 status code if successful, an error message with appropriate
            HTTP status code otherwise.
        """
        if backup_id in self._mounts.keys():
            return self._unmount_backup(backup_id)
        else:
            return 'The specified backup is not mounted.', 400

    def _unmount_backup(self, backup_id):
        try:
            self._mounts[backup_id]['controller'].unmount()
            self._mounts.pop(backup_id)
            return 'OK', 200
        except Exception as e:
            return 'Cannot unmount the backup, cause: ' + str(e), 400
