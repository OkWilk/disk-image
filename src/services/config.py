import constants
from configparser import ConfigParser


class _ConfigHelper:
    def __init__(self):
        self.config = ConfigParser()
        self._read_config(constants.CONFIG_FILE)

    def _read_config(self, file):
        self.config.read(file)


ConfigHelper = _ConfigHelper()
