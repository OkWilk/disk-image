"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from flask_restful import Resource

import constants
from monitoring.plugins import DiskSpacePlugin, RAMUtilisationPlugin, CpuUtilisationPlugin, DiskIOUtilisationPlugin
from monitoring.sysmon import SystemMonitor


class Monitor(Resource):
    """ Defines the Web API for retrieving utilisation metrics from the Imaging Node. """
    MONITOR = SystemMonitor()
    MONITOR.add_plugin(DiskSpacePlugin())
    MONITOR.add_plugin(RAMUtilisationPlugin())
    MONITOR.add_plugin(CpuUtilisationPlugin(constants.METRIC_INTERVAL))
    MONITOR.add_plugin(DiskIOUtilisationPlugin(constants.DISK_IO_INTERVAL))

    def get(self):
        """
        Provides information regarding utilisation of the Imaging Nodes.
        :return: a JSON object containing metrics collected and 200 HTTP status.
        """
        return self.MONITOR.get_metrics(), 200
