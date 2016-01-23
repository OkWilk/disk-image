from abc import ABCMeta, abstractmethod
from threading import Thread, Lock


class MetricPlugin:
    __metaclass__ = ABCMeta
    NAME = ''

    def __init__(self):
        self.name = self.NAME
        self._value = 0

    @property
    def value(self):
        self._value = self._collect_metric()
        return self._value

    @abstractmethod
    def _collect_metric(self):
        pass


class ThreadedMetricPlugin(MetricPlugin):
    __metaclass__ = ABCMeta

    def __init__(self, interval):
        MetricPlugin.__init__(self)
        self.interval = interval
        self._thread = None
        self._stop = False;
        self._lock = Lock()
        self._run()

    @abstractmethod
    def _run(self):
        pass

    @property
    def value(self):
        with self._lock:
            return self._value

    def stop(self):
        self.stop = True;
        self._thread.join()


class SystemMonitor:
    def __init__(self):
        self._plugins = []

    def get_metrics(self):
        metrics = {}
        for plugin in self._plugins:
            metrics[plugin.name] = plugin.value
        return metrics

    def add_plugin(self, plugin):
        self._plugins.append(plugin)

    def remove_plugin(self, plugin):
        self._plugins.remove(plugin)

    def remove_all_plugins(self):
        self._plugins = self._plugins[:]
