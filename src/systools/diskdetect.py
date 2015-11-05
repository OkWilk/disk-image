from src.systools.runcommand import Execute, OutputParser
import re
import logging

_LSBLK_COLUMNS = ['KNAME', 'TYPE', 'FSTYPE', 'SIZE']


class _DiskParser(OutputParser):

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
        devices = self._remove_ignored_device_types(devices)
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
            except AttributeError:
                if(keyword == 'KNAME'):
                    logging.error('Cannot detect drive name in the line: "' +
                                  line + '".')
                    raise ValueError()
                else:
                    pass  # The line did not contain secondary key
        return extracted

    def _group_by_device(self, extracted:list) -> dict:
        """Processes the list of drives and partitions in order to group them."""
        result = dict()
        for record in extracted:
            if not(record['TYPE'] == 'part'):
                partitions = list()
                name = record['KNAME']
                result[name] = {
                    'size': record['SIZE'],
                    'type': record['TYPE'],
                    'partitions': partitions
                }
            elif(record['TYPE'] == 'part' and name in record['KNAME']):
                partition = {
                    'name': record['KNAME'],
                    'size': record['SIZE'],
                    'fs': record['FSTYPE']
                }
                partitions.append(partition)
        return result

    def _remove_ignored_device_types(self, devices:dict) -> dict:
        """Returns a new dictionary without the ignored devices specified
        in the ignored_devices list"""
        return {i:devices[i] for i in devices if devices[i]['type']
                not in self.ignore_list}


def detect_disks() -> dict:
    """Detects disks recognised by the operating system and returns them along
    with additional information such as size, partitions and file systems."""
    command = ['lsblk', '--output', ','.join(_LSBLK_COLUMNS), '--pairs', '--bytes']
    runner = Execute(command, _DiskParser())
    runner.run()
    return runner.output()
