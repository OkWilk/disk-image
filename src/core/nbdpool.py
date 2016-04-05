from os import listdir
from threading import Lock
from time import sleep

import constants
from core.runcommand import Execute
from lib.thread import ExtendedThread


class NBDNode:
    def __init__(self, device):
        self.device = device
        self.mountpoint = ""
        self.error = False
        self._runner = None
        self._thread = None
        self._unmounting = False

    def mount(self, image, fs, mountpoint):
        self.mountpoint = mountpoint
        command = ['imagemount', '-f', image, '-d', self.device, '-m', mountpoint, '-t', fs, '-r', '-D']
        self._runner = Execute(command)
        self._thread = ExtendedThread(exception_callback=self._exception_callback, target=self._mount_command,
                                      args=(image, fs, mountpoint), daemon=True)
        self._thread.start()
        self._wait_and_set_status()

    def _wait_and_set_status(self):
        while self._runner.poll() is Execute.PROCESS_NOT_STARTED:
            sleep(constants.BUSY_WAIT_INTERVAL)
        if self._runner.poll() is not Execute.PROCESS_RUNNING:
            self.error = True

    def _mount_command(self, image, fs, mountpoint):
        self._runner.run()
        status = self._runner.poll()
        if not self._unmounting and status != 0:
            raise MountException("Mounting returned exit code different than 0")

    def _exception_callback(self, source, e):
        self.error = True

    def unmount(self):
        if self._thread.isAlive():
            self._unmounting = True
            self._runner.kill()
            self._thread.join()
            self._unmounting = False
        command = ['umount', self.mountpoint]
        Execute(command).run()

    def reset(self):
        self.unmount()
        self._runner = None
        self._thread = None
        self.error = False


class _NBDPool:
    def __init__(self):
        self._lock = Lock()
        self._nbd_nodes = []
        self._used_nodes = []
        self._load_nodes()

    def _load_nodes(self):
        devices = listdir(constants.DEVICE_PATH)
        for device in devices:
            if 'nbd' in device:
                self._nbd_nodes.append(NBDNode(constants.DEVICE_PATH + device))

    def acquire(self):
        with self._lock:
            try:
                node = self._nbd_nodes.pop()
                self._used_nodes.append(node)
                return node
            except IndexError:
                raise Exception("Not enough resources to mount all partitions of this backup. "
                                "Unmount other backups and try again.")

    def release(self, node):
        with self._lock:
            try:
                self._used_nodes.remove(node)
                node.reset()
                self._nbd_nodes.append(node)
            except:
                raise

class MountException(BaseException):
    pass

# initialise singleton
NBDPool = _NBDPool()


