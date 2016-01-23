import psutil
import constants
from threading import Thread
from core.runcommand import Execute, OutputParser
from .sysmon import MetricPlugin, ThreadedMetricPlugin


class DiskSpacePlugin(MetricPlugin):
    NAME = 'DiskSpace'
    INDEX = 3

    def _collect_metric(self):
        return psutil.disk_usage(constants.BACKUP_PATH)[self.INDEX]


class RAMUtilisationPlugin(MetricPlugin):
    NAME = 'RAM_Utilisation'
    INDEX = 2

    def _collect_metric(self):
        return psutil.virtual_memory()[self.INDEX]


class CpuUtilisationPlugin(ThreadedMetricPlugin):
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
    NAME = 'Disk_IO_Utilisation'
    COMMAND = ['iostat', '-x', '-d', 'sda']

    def _run(self):
        self._thread = Thread(target=self._collect_metric, daemon=True)
        self._thread.start()

    @property
    def value(self):
        with self._lock:
            output = self._runner.output()
            if output:
                self._value = output
            return self._value

    def _collect_metric(self):
        self.COMMAND.append(str(self.interval))
        self._runner = Execute(self.COMMAND, output_parser=_IOStatParser(), use_pty=True)
        self._runner.run()

    def stop(self):
        self.stop = True;
        self._runner.kill()
        self._thread.join()

class _IOStatParser(OutputParser):
    def __init__(self):
        self.output = None

    def parse(self, data):
        """Processes data from the command output and saves the result as output."""
        self.metrics = data.strip().split('\n')
        self.metrics = self.metrics[-1].split(' ')
        self.output = self.metrics[-1]

