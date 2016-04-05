from flask_restful import Resource, abort, reqparse
from core.controller import BackupController, RestorationController
import constants
from lib.exceptions import DetectionException

BACKUP_OPERATION = 'Backup'
RESTORATION_OPERATION = 'Restoration'

_jobs = {}


class Job(Resource):
    _parser = reqparse.RequestParser()
    _parser.add_argument('job_id', type=str, location='json')
    _parser.add_argument('operation', type=str, location='json')
    _parser.add_argument('disk', type=str, location='json')
    _parser.add_argument('overwrite', type=bool, location='json')
    _parser.add_argument('rescue', type=bool, location='json')
    _parser.add_argument('space_check', type=bool, location='json')
    _parser.add_argument('fs_check', type=bool, location='json')
    _parser.add_argument('crc_check', type=bool, location='json')
    _parser.add_argument('force', type=bool, location='json')
    _parser.add_argument('compress', type=bool, location='json')

    def get(self, job_id=None):
        if job_id:
            return self._get_job_details(job_id)
        else:
            return self._get_job_list()

    def _get_job_list(self):
        payload = []
        if _jobs:
            for key in _jobs.keys():
                payload.append(self._get_job_details(key))
        return payload

    def _get_job_details(self, job_id):
        if job_id in _jobs:
            payload = {
                'id': job_id,
                'disk': _jobs[job_id]['disk']
            }
            payload.update(_jobs[job_id]['controller'].get_status())
            return payload
        else:
            return "The requested job does not exist.", 404

    def post(self):
        args = self._parser.parse_args(strict=True)
        config = self._build_config_from_request_args(args)
        if args['job_id'] and args['disk'] and args['operation']:
            try:
                controller = self._get_controller(args['operation'], args['disk'], args['job_id'], config)
                controller.run()
                _jobs[args['job_id']] = {'disk': args['disk'], 'controller': controller}
                return "OK", 200
            except DetectionException as e:
                return str(e), 400
        else:
            abort(400, message="Error: Invalid input detected.")

    def _get_controller(self, operation, disk, job_id, config):
        if operation == BACKUP_OPERATION:
            return BackupController(disk, job_id, config)
        elif operation == RESTORATION_OPERATION:
            return RestorationController(disk, job_id, config)

    def _build_config_from_request_args(self, args):
        config = self._build_config_with_defaults()
        if 'overwrite' in args:
            config['overwrite'] = args['overwrite']
        if 'rescue' in args:
            config['rescue'] = args['rescue']
        if 'space_check' in args:
            config['space_check'] = args['space_check']
        if 'fs_check' in args:
            config['fs_check'] = args['fs_check']
        if 'crc_check' in args:
            config['crc_check'] = args['crc_check']
        if 'force' in args:
            config['force'] = args['force']
        if 'compress' in args:
            config['compress'] = args['compress']
        return config

    def _build_config_with_defaults(self):
        config = {
            'overwrite': False,
            'rescue': False,
            'space_check': True,
            'fs_check': True,
            'crc_check': True,
            'force': False,
            'refresh_delay': 5,
            'compress': False,
        }
        return config

    def delete(self, job_id):
        if job_id in _jobs:
            return self._finish_job(job_id)
        else:
            return "Error: Invalid resource requested.", 404

    def _finish_job(self, job_id):
        status = self.get(job_id)['status']
        if status == constants.STATUS_FINISHED or status == constants.STATUS_ERROR:
            _jobs.pop(job_id)
            return 'OK', 200
        else:  # TODO: allow terminating jobs and deleting them.
            return 'Cannot abort job at this moment.', 400

