from src.runcommand import Execute, OutputParser
import re


class DiskParser(OutputParser):
    def __init__(self, ignore_list=['loop','rom']):
        self.ignore_list = ignore_list
        self.output = {}
        self._keywords = ['NAME', 'TYPE', 'FSTYPE', 'SIZE']

    def parse(self, data):
        output_lines = data.split('\n')
        for line in output_lines:
            self._extract_pairs(line)

    def _extract_pairs(self, string):
        d = dict()
        for key in self._keywords:
            try:
                pair = re.search(r'\b' + key + r'\b="[^"]*"', string).group(0)
                d[key] = pair.split('=')[1].strip().strip('"')
            except AttributeError:
                pass  # The line did not contain required keys
        print(str(d))

def detect_disks():
    """Detects disk drives recognised by the operating system"""
    command = ['lsblk', '--output', 'NAME,TYPE,FSTYPE,SIZE', '--pairs', '--bytes']
    runner = Execute(command,DiskParser())
    runner.run()
    return runner.output()
"""
format:
{
    disk1: {
        name: 'sda',
        type: 'disk'
        partitions: {
            part1: {
                name: 'sda1',
                fs: 'ext3'
                }
            part2: {
                name: 'sda2',
                fs: 'swap'
                }
            }
        }
    disk2: {
        name: 'sdb',
        type: 'disk'
        }
}
"""
