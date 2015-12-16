from flask_restful import Resource, abort, reqparse, fields, marshal_with
from diskutils.controller import ProcessController

backups = {}
_backups = {}


class Backup(Resource):
    _parser = reqparse.RequestParser()
    _parser.add_argument('job_id', type=str, location='json')
    _parser.add_argument('source', type=str, location='json')
    _parser.add_argument('overwrite', type=bool, location='json')
    _parser.add_argument('rescue', type=bool, location='json')
    _parser.add_argument('space_check', type=bool, location='json')
    _parser.add_argument('fs_check', type=bool, location='json')
    _parser.add_argument('crc_check', type=bool, location='json')
    _parser.add_argument('force', type=bool, location='json')
    _parser.add_argument('refresh_delay', type=int, location='json')
    _parser.add_argument('compress', type=bool, location='json')

    def get(self):
        if backups:
            for key in backups.keys():
                backups[key]['details'] = _backups[key]['controller'].get_status()
        return backups

    def post(self):
        args = self._parser.parse_args(strict=True)
        config = self._build_config_from_request_args(args)
        print(str(config))
        if args['job_id'] and args['source']:
            controller = ProcessController(args['source'], args['job_id'], config)
            controller.backup()
            _backups[args['job_id']] = {'source': args['source'], 'controller': controller}
            backups[args['job_id']] = {'source': args['source']}
            return "OK", 200
        else:
            abort(400, message="Error: Invalid input detected.")

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


class BackupDetails(Resource):
    def get(self, backup_id):
        if backup_id in _backups:
            return _backups[backup_id]['controller'].get_status()
        else:
            abort(400, message="Error: Invalid input detected.")

    def delete(self, backup_id):
        if backup_id in _backups:
            _backups.pop(backup_id)
            backups.pop(backup_id)
