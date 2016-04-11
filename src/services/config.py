"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from configparser import ConfigParser

import constants


class _ConfigHelper:
    """
    This class encapsulates the reading and parsing logic for the config file.
    """
    def __init__(self):
        self.config = ConfigParser()
        self._read_config(constants.CONFIG_FILE)

    def _read_config(self, file):
        self.config.read(file)


# Export as Singleton
ConfigHelper = _ConfigHelper()
