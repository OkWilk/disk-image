from flask import Flask
from flask_restful import Api, Resource, abort, reqparse, fields, marshal_with
from threading import Thread
from systools.diskdetect import detect_disks
from systools.image import PartitionImage

app = Flask(__name__)
api = Api(app)

backups = []
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
    _parser.add_argument('jobID')
    _parser.add_argument('source')

    # _resource_fields = {
    #     'jobID': fields.N,
    #     'source': fields.Nested,
    # }

    # @marshal_with(_resource_fields)
    def get(self):
        return backups

    def post(self):
        args = self._parser.parse_args(strict=True)
        if args['jobID'] and args['source']:
            imager = PartitionImage(args['source'], '/tmp/images/')
            t = Thread(target=imager.backup)
            t.start()
            _backups[args['jobID']] = {'source': args['source'], 'thread': t, 'imager': imager}
            backups.append({'jobID':args['jobID'], 'source': args['source']})
            return "OK", 200
        else:
            return "Error: Invalid input detected.", 400


class BackupDetails(Resource):
        def get(self, backup_id):
            return _backups[backup_id]['imager'].get_status()


api.add_resource(Disk, '/disk')
api.add_resource(DiskDetails, '/disk/<disk_id>')
api.add_resource(Backup, '/job/backup')
api.add_resource(BackupDetails, '/job/backup/<backup_id>')


if __name__ == '__main__':
    app.run(debug=True)
