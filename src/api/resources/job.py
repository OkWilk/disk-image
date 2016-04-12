"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from threading import Thread

from flask_restful import Resource, reqparse

import constants
from core.controller import BackupController, RestorationController


class Job(Resource):
    """ Defines the Web API for creating, retrieving and deleting jobs on the Imaging Node. """
    BACKUP_OPERATION = 'Backup'
    RESTORATION_OPERATION = 'Restoration'

    _jobs = {}

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
        """
        Provides information about jobs or a single job being executed on the node.
        :param job_id: string defining the name of the job to be retrieved.
        :return: a list of jobs with statuses or a single job if job_id is present.
        """
        if job_id:
            return self._get_job_details(job_id)
        else:
            return self._get_job_list()

    def _get_job_list(self):
        payload = []
        if self._jobs:
            for key in self._jobs.keys():
                payload.append(self._get_job_details(key))
        return payload

    def _get_job_details(self, job_id):
        if job_id in self._jobs:
            payload = {
                'id': job_id,
                'disk': self._jobs[job_id]['disk']
            }
            payload.update(self._jobs[job_id]['controller'].get_status())
            return payload
        else:
            return "The requested job does not exist.", 404

    def post(self):
        """
        Facilitates creation of new jobs by sending HTTP POST request with JSON body.
        The JSON is expected to provide the type of the job to be created as well as
        a number of parameters defined by the request parser.
        :return: Ok with 200 if job was scheduled successfully,
            Error message with an appropriate HTTP status if job cannot be started.
        """
        args = self._parser.parse_args(strict=True)
        config = self._build_config_from_request_args(args)
        if args['job_id'] and args['disk'] and args['operation']:
            try:
                if args['job_id'] not in self._jobs.keys():
                    controller = self._get_controller(args['operation'], args['disk'], args['job_id'], config)
                    controller.run()
                    self._jobs[args['job_id']] = {'disk': args['disk'], 'controller': controller}
                    return "OK", 200
                else:
                    return "A job with id '" + args['job_id'] + "' is already running on this node.", 400
            except Exception as e:
                return str(e), 400
        else:
            return "Error: Invalid input detected.", 400

    def _get_controller(self, operation, disk, job_id, config):
        if operation == self.BACKUP_OPERATION:
            return BackupController(disk, job_id, config)
        elif operation == self.RESTORATION_OPERATION:
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
            'refresh_delay': constants.REFRESH_DELAY,
            'compress': False,
        }
        return config

    def delete(self, job_id):
        """
        Removes finished jobs. If a job is running, it will be cancelled instead.
        :param job_id: the string identifier of the job to be removed or cancelled.
        :return: OK with 200 status code if successful, an error with appropriate
            HTTP status code otherwise.
        """
        if job_id in self._jobs:
            return self._finish_job(job_id)
        else:
            return "Error: Invalid resource requested.", 404

    def _finish_job(self, job_id):
        status = self.get(job_id)['status']
        if status == constants.STATUS_FINISHED or status == constants.STATUS_ERROR:
            self._jobs.pop(job_id)
            return 'OK', 200
        else:
            try:
                ctrl = self._jobs[job_id]['controller']
                t = Thread(target=ctrl.kill, daemon=True)
                t.start()
                return 'Please wait, while the job is being cancelled.', 202
            except:
                return 'Cannot abort job at this moment.', 400
