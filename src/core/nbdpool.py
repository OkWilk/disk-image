import constants
from core.runcommand import Execute
from threading import Thread, Lock
from os import listdir


class NBDNode:
    def __init__(self, device):
        self.device = device
        self.mountpoint = ""
        self._runner = None
        self._thread = None

    def mount(self, image, fs, mountpoint):
        self.mountpoint = mountpoint
        command = ['imagemount', '-f', image, '-d', self.device, '-m', mountpoint, '-t', fs, '-r', '-D']
        self._runner = Execute(command)
        self._thread = Thread(target=self._runner.run)
        self._thread.start()

    def unmount(self):
        if self._thread.isAlive():
            self._runner.kill()
            self._thread.join()
        command = ['umount', self.mountpoint]
        Execute(command).run()

    def reset(self):
        self.unmount()
        self._runner = None
        self._thread = None


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
        self._lock.acquire()
        try:
            node = self._nbd_nodes.pop()
            self._used_nodes.append(node)
            return node
        except IndexError:
            raise
        finally:
            self._lock.release()

    def release(self, node):
        self._lock.acquire()
        try:
            self._used_nodes.remove(node)
            node.reset()
            self._nbd_nodes.append(node)
        except:
            raise
        finally:
            self._lock.release()

# initialise singleton
NBDPool = _NBDPool()
