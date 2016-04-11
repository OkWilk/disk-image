"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from flask_restful import Resource

from core.diskdetect import DiskDetect


class Disk(Resource):
    """ Defines the Web API for retrieving and manipulating information
        regarding disks connected to the Imaging Node
    """
    def get(self, disk_id=None):
        """
        Provides information regarding the disks attached to the Imaging Node.
        :param disk_id: string containing the name of the disk in Linux format (e.g. sda)
        :return: If disk_id is provided a JSON object containing the disk information such as
            disk size, device type and detected partitions is returned. Otherwise a list of
            disks with the above details is returned.
        """
        if disk_id:
            try:
                return DiskDetect.get_disk_details(disk_id)
            except:
                return "Disk " + disk_id + " does not exist.", 404
        else:
            return DiskDetect.get_disk_list()
