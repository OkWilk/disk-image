"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from threading import Thread

import psutil

from core.runcommand import Execute, OutputParser
from services.config import ConfigHelper
from .sysmon import MetricPlugin, ThreadedMetricPlugin


class DiskSpacePlugin(MetricPlugin):
    """
    This plugin collects disk space utilisation, it does represent the percent of the disk
    being used.
    """
    NAME = 'DiskSpace'
    INDEX = 3

    def _collect_metric(self):
        return psutil.disk_usage(ConfigHelper.config['node']['backup_path'])[self.INDEX]


class RAMUtilisationPlugin(MetricPlugin):
    """
    This plugin collects RAM utilisation, it does represent the percent of the memory
    being used.
    """
    NAME = 'RAM_Utilisation'
    INDEX = 2

    def _collect_metric(self):
        return psutil.virtual_memory()[self.INDEX]


class CpuUtilisationPlugin(ThreadedMetricPlugin):
    """
    This plugin collects CPU utilisation, it does represent the average processor usage over the
    interval period in percents.
    """
    NAME = 'CPU_Utilisation'

    def _run(self):
        self._thread = Thread(target=self._set_cpu_value, daemon=True)
        self._thread.start()

    def _collect_metric(self):
        return psutil.cpu_percent(self.interval)

    def _set_cpu_value(self):
        while not self._stop:
            value = self._collect_metric()
            with self._lock:
                self._value = value


class DiskIOUtilisationPlugin(ThreadedMetricPlugin):
    """
    This plugin collects the disk I/O utilisation, it does represent the average I/O
    usage of the backup disk over the interval period in percents.
    """
    NAME = 'Disk_IO_Utilisation'
    COMMAND = ['iostat', '-x', '-d', str(ConfigHelper.config['node']['backup_disk'])]

    def _run(self):
        self._thread = Thread(target=self._collect_metric, daemon=True)
        self._thread.start()

    @property
    def value(self):
        """
        Checks whether a new value is available and returns the up to date value of the metric.
        :return: numerical value representing the percentage of the I/O utilisation.
        """
        with self._lock:
            output = self._runner.output()
            if output:
                self._value = output
            return float(self._value)

    def _collect_metric(self):
        self.COMMAND.append(str(self.interval))
        self._runner = Execute(self.COMMAND, output_parser=_IOStatParser(), use_pty=True)
        self._runner.run()

    def stop(self):
        """
        Stops the command that is executed in the background to collect the metric.
        :return: None
        """
        self._stop = True
        self._runner.kill()
        self._thread.join()

class _IOStatParser(OutputParser):
    """ The output parser class for extracting I/O utilisation from the iostat command """
    def __init__(self):
        self.output = None

    def parse(self, data):
        """Processes data from the command output and saves the result as output."""
        self.metrics = data.strip().split('\n')
        self.metrics = self.metrics[-1].split(' ')
        self.output = self.metrics[-1]


