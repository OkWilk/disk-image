from flask import request
from flask_restful import Resource

import constants
from core.controller import MountController

mounted = {}


class Mount(Resource):

    def get(self, backup_id=None):
        if backup_id:
            return self._get_mount_details(backup_id)
        else:
            return self._get_mount_list()

    def _get_mount_details(self, backup_id):
        for id in mounted.keys():
            if id == backup_id:
                return mounted[id]['controller'].get_status(), 200
        return 'Requested backup is not mounted on this node.', 404

    def _get_mount_list(self):
        payload = []
        for mount in mounted.values():
            payload.append(mount['controller'].get_status())
        return payload, 200

    def post(self):
        data = request.get_json(force=True)
        if 'backup_id' in data:
            if data['backup_id'] not in mounted.keys():
                return self._mount_backup(data['backup_id'])
            else:
                return 'The requested backup is already mounted.', 400
        else:
            return 'Invalid request format, the required backup_id field was not provided.', 400

    def _mount_backup(self, backup_id):
        try:
            controller = MountController(backup_id)
            controller.mount()
            print(controller.get_status()['status'])
            if controller.get_status()['status'] != constants.STATUS_ERROR:
                mounted[backup_id] = {'controller': controller}
                return 'OK', 200
            else:
                return controller.get_status()['error_msg'], 500
        except Exception as e:
            return 'Cannot mount backup, cause: ' + str(e), 400

    def delete(self, backup_id):
        if backup_id in mounted.keys():
            return self._unmount_backup(backup_id)
        else:
            return 'The specified backup is not mounted.', 400

    def _unmount_backup(self, backup_id):
        try:
            mounted[backup_id]['controller'].unmount()
            mounted.pop(backup_id)
            return 'OK', 200
        except Exception as e:
            return 'Cannot unmount the backup, cause: ' + str(e), 400
