from flask_restful import Resource, abort, reqparse
from core.controller import BackupController, RestorationController
import constants

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
    _parser.add_argument('refresh_delay', type=int, location='json')
    _parser.add_argument('compress', type=bool, location='json')

    def get(self):
        payload = []
        details = JobDetails()
        if _jobs:
            for key in _jobs.keys():
                payload.append(details.get(key))
        return payload

    def post(self):
        args = self._parser.parse_args(strict=True)
        config = self._build_config_from_request_args(args)
        if args['job_id'] and args['disk'] and args['operation']:
            controller = self._get_controller(args['operation'], args['disk'], args['job_id'], config)
            controller.run()
            _jobs[args['job_id']] = {'disk': args['disk'], 'controller': controller}
            return "OK", 200
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
        if 'refresh_delay' in args:
            config['refresh_delay'] = args['refresh_delay']
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


class JobDetails(Resource):
    def get(self, job_id):
        if job_id in _jobs:
            payload = {
                'id': job_id,
                'disk': _jobs[job_id]['disk']
            }
            payload.update(_jobs[job_id]['controller'].get_status())
            return payload
        else:
            abort(400, message="Error: Invalid input detected.")

    def delete(self, job_id):
        if job_id in _jobs:
            self._finish_backup(job_id)
            return "OK", 200
        else:
            abort(400, message="Error: Invalid resource requested.")

    def _finish_backup(self, job_id):
        status = self.get(job_id)
        if status['status'] == constants.STATUS_FINISHED:
            _jobs.pop(job_id)
            return 'OK', 200
        else:  # TODO: allow terminating jobs and deleting them.
            abort(400, message="Cannot abort job at this moment.")
