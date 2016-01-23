from flask_restful import Resource

import constants
from monitoring.plugins import DiskSpacePlugin, RAMUtilisationPlugin, CpuUtilisationPlugin, DiskIOUtilisationPlugin
from monitoring.sysmon import SystemMonitor


class Monitor(Resource):
    MONITOR = SystemMonitor()
    MONITOR.add_plugin(DiskSpacePlugin())
    MONITOR.add_plugin(RAMUtilisationPlugin())
    MONITOR.add_plugin(CpuUtilisationPlugin(constants.METRIC_INTERVAL))
    MONITOR.add_plugin(DiskIOUtilisationPlugin(1))

    def get(self):
        return self.MONITOR.get_metrics()
