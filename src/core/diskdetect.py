"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

import logging
import re
from threading import Lock

from .runcommand import Execute, OutputParser

_LSBLK_COLUMNS = ['KNAME', 'TYPE', 'FSTYPE', 'SIZE']


class _LsblkOutputParser(OutputParser):
    """The specialised parsing class to be used with lsblk list outputs."""

    def __init__(self, ignore_list:list=['nbd', 'loop', 'rom']):
        self.ignore_list = ignore_list
        self.output = {}

    def parse(self, data):
        """
        Parses output of lsblk list format into a dictionary of devices
        and their respective partitions. The result of this operation will be
        stored in the output variable.
        :param data: output string from the executed command
        :return: None
        """
        output_lines = data.split('\n')
        parsed_lines = list()
        for line in output_lines:
            if(line):
                extracted = self._extract_pairs_to_dict(line)
                parsed_lines.append(extracted)
        devices = self._group_by_device(parsed_lines)
        self.output = devices

    def _extract_pairs_to_dict(self, line):
        """
        Extracts key value pairs from the string and returns them as a
        dictionary. The device name key is required. In case of line that does
        not contain KNAME key ValueError will be raised.
        :param line: a single line of the output.
        :return: dictionary containing key-value paris extracted from the line
        """
        extracted = dict()
        for keyword in _LSBLK_COLUMNS:
            try:
                pair = re.search(r'\b' + keyword + r'\b="[^"]*"', line).group(0)
                key, value = pair.split('=')
                extracted[keyword] = value.strip().strip('"')
            except Exception:
                if(keyword == 'KNAME'):
                    logging.error('Cannot detect drive name in the line: "' +
                                  line + '".')
                    raise ValueError()
                else:
                    pass  # The line did not contain secondary key
        return extracted

    def _group_by_device(self, extracted:list) -> dict:
        """
        Processes the list of drives and partitions in order to group them.
        :param extracted: list of extracted drives and partitions
        :return: list of dictionaries of disks with partitions
        """
        result = []
        for record in extracted:
            if self._is_accepted(record):
                if not(record['TYPE'] == 'part'):
                    partitions = list()
                    record = {
                        'name': record['KNAME'],
                        'size': record['SIZE'],
                        'type': record['TYPE'],
                        'partitions': partitions
                    }
                    result.append(record)
                else:
                    partition = {
                        'name': record['KNAME'],
                        'size': record['SIZE'],
                        'fs': record['FSTYPE']
                    }
                    partitions.append(partition)
        return result

    def _is_accepted(self, record):
        if record['TYPE'] in self.ignore_list:
            return False
        for ignored in self.ignore_list:
            if ignored in record['KNAME']:
                return False
        return True


class _DiskDetect:
    _COMMAND = ['lsblk', '--output', ','.join(_LSBLK_COLUMNS), '--pairs', '--bytes']

    def __init__(self):
        self._runner = Execute(self._COMMAND, _LsblkOutputParser())
        self._lock = Lock()
        self._output = ''

    def get_disk_list(self):
        """
        Detects disks recognised by the operating system and returns them along
        with additional information such as size, partitions and file systems.
        :return: list of disks with additional information regarding partitions.
        """
        return self._detect_disks()

    def get_disk_details(self, disk_id):
        """
        Detects disk and returns details for the disk with the specified disk_id.
        :param disk_id: string identifier of the disk details to be returned (e.g. sda)
        :return: dictionary containing information regarding the selected disk.
        :exception: ValueError is raised if no disks match the provided disk_id.
        """
        disk_list = self._detect_disks()
        for disk in disk_list:
            if disk['name'] == disk_id:
                return disk
        raise ValueError("Disk " + disk_id + " was not detected by the system.")

    def _detect_disks(self):
        with self._lock:
            try:
                self._runner.run()
                self._output = self._runner.output()
            except Exception as e:
                logging.error("Disk detection failed, cause: " + str(e))
                raise e
            return self._output


# Export as singleton
DiskDetect = _DiskDetect()
