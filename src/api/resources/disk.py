from diskutils.diskdetect import detect_disks
from flask_restful import Resource


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
