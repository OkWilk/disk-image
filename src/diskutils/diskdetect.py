"""Author: Oktawiusz Wilk
Date: 05/11/2015
"""

from .runcommand import Execute, OutputParser
import re
import logging

_LSBLK_COLUMNS = ['KNAME', 'TYPE', 'FSTYPE', 'SIZE']
_COMMAND = ['lsblk', '--output', ','.join(_LSBLK_COLUMNS), '--pairs', '--bytes']


def _detect_disks():
    runner = Execute(_COMMAND, _LsblkOutputParser())
    runner.run()
    return runner.output()


def get_disk_list():
    """Detects disks recognised by the operating system and returns them along
    with additional information such as size, partitions and file systems."""
    return _detect_disks()


def get_disk_details(disk_id):
    disk_list = _detect_disks()
    for disk in disk_list:
        print(str(disk['name']) + ' == ' +str(disk_id) + '=' + str(disk['name'] == disk_id))
        if disk['name'] == disk_id:
            return disk
    raise ValueError("Disk " + disk + " was not detected by the system.")


class _LsblkOutputParser(OutputParser):
    """The specialised parsing class to be used with lsblk list outputs."""

    def __init__(self, ignore_list:list=['loop', 'rom']):
        """Add parsing configuration to the object.
        ignore_list - a list of device types to be ignored: drive, part, loop, rom
        """
        self.ignore_list = ignore_list
        self.output = {}

    def parse(self, data:str):
        """Parses output of lsblk list format into a dictionary of devices
        and their respective partitions. The result of this operation will be
        stored in the output variable.
        """
        output_lines = data.split('\n')
        parsed_lines = list()
        for line in output_lines:
            if(line):
                extracted = self._extract_pairs_to_dict(line)
                parsed_lines.append(extracted)
        devices = self._group_by_device(parsed_lines)
        self.output = devices

    def _extract_pairs_to_dict(self, line:str) -> dict:
        """Extracts key value pairs from the string and returns them as a
        dictionary. The device name key is required. In case of line that does
        not contain KNAME key ValueError will be raised.
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
        """Processes the list of drives and partitions in order to group them."""
        result = []
        for record in extracted:
            if self._is_accepted_device_type(record['TYPE']):
                if not(record['TYPE'] == 'part'):
                    partitions = list()
                    name = record['KNAME']
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

    def _is_accepted_device_type(self, device):
        return device not in self.ignore_list
