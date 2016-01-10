from diskutils.diskdetect import get_disk_list, get_disk_details
from flask_restful import Resource, abort


class Disk(Resource):
    def get(self, disk_id=None):
        if disk_id:
            try:
                return get_disk_details(disk_id)
            except:
                abort(404, message="Disk {} doesn't exist".format(disk_id))
        else:
            return get_disk_list()
