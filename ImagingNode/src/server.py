from flask import Flask
from flask_restful import Api, Resource, abort, reqparse, fields, marshal_with
from threading import Thread
from diskutils.diskdetect import detect_disks
from diskutils.controller import ProcessController

app = Flask(__name__)
api = Api(app)

backups = {}
_backups = {}
restorations = {}
mounts = {}


class Disk(Resource):
    def get(self):
        return detect_disks()


class DiskDetails(Resource):
    def get(self, disk_id):
        disks = detect_disks()
        if disk_id in disks:
            return disks[disk_id]
        else:
            abort(404, message="Disk {} doesn't exist".format(disk_id))


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
            return "Error: Invalid input detected.", 400

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
            return "Error: Invalid input detected.", 400

api.add_resource(Disk, '/disk')
api.add_resource(DiskDetails, '/disk/<disk_id>')
api.add_resource(Backup, '/job/backup')
api.add_resource(BackupDetails, '/job/backup/<backup_id>')


if __name__ == '__main__':
    app.run(debug=True)
