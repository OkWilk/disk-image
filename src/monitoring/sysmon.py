"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from abc import ABCMeta, abstractmethod
from threading import Lock


class MetricPlugin:
    """
    This class provides a basic structure for all metric plugins
    """
    __metaclass__ = ABCMeta
    NAME = ''

    def __init__(self):
        self.name = self.NAME
        self._value = 0

    @property
    def value(self):
        """
        This property is used to execute the logic required to collect the data and to apply
        additional processing if necessary.
        :return: ready metric value
        """
        self._value = self._collect_metric()
        return self._value

    @abstractmethod
    def _collect_metric(self):
        pass


class ThreadedMetricPlugin(MetricPlugin):
    """
    This class defines the structure for plugins which need to collect data over a period of time
    and perform aggregation. The plugin will execute the command in the background on a separate
    thread.
    """
    __metaclass__ = ABCMeta

    def __init__(self, interval):
        MetricPlugin.__init__(self)
        self.interval = interval
        self._thread = None
        self._stop = False
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
        """
        Stops processing of the background thread for the command execution
        :return: None
        """
        self._stop = True
        self._thread.join()


class SystemMonitor:
    """
    The SystemMonitor is the plugin handler that allows gathering metrics from all
    of the plugins at once.
    """
    def __init__(self):
        self._plugins = []

    def get_metrics(self):
        """
        Builds a dictionary with plugin name as a key and the metric value as a value.
        :return: dictionary of key-value pairs for all enabled plugins.
        """
        metrics = {}
        for plugin in self._plugins:
            metrics[plugin.name] = plugin.value
        return metrics

    def add_plugin(self, plugin):
        """
        Adds a MetricPlugin instance to be queried for information at later stage.
        :param plugin: an instance of a subclass of the MetricPlugin
        :return: None
        """
        self._plugins.append(plugin)

    def remove_plugin(self, plugin):
        """
        Removes a MetricPlugin instance from the list of the plugins to be queried for information.
        :param plugin: an instance of a subclass of the MetricPlugin
        :return: None
        """
        self._plugins.remove(plugin)

    def remove_all_plugins(self):
        """
        Removes all previously added plugins.
        :return: None
        """
        self._plugins = self._plugins[:]
