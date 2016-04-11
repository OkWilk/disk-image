"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from os import listdir
from threading import Lock
from time import sleep

import constants
from core.runcommand import Execute
from lib.exceptions import MountException
from lib.thread import ExtendedThread


class NBDNode:
    """
    This class wraps NBD devices provided by the Linux kernel and allows mounting of
    the partclone images with the use of the ImageMount command.
    """

    def __init__(self, device):
        self.device = device
        self.mountpoint = ""
        self.error = False
        self._runner = None
        self._thread = None
        self._unmounting = False

    def mount(self, image, fs, mountpoint):
        """
        Mounts the specified image file at the given mountpoint.
        :param image: path to the image file to be mounted.
        :param fs: file system of the imaged partition.
        :param mountpoint: directory to be used for mounting.
        :return: None
        """
        self.mountpoint = mountpoint
        self._thread = ExtendedThread(exception_callback=self._exception_callback,
                                      target=self._mount_command, args=(image, fs, mountpoint),
                                      daemon=True)
        self._thread.start()
        self._wait_and_set_status()

    def _wait_and_set_status(self):
        """
        Waits until the image is mounted and checks whether the mount procedure
        was successful or not.
        :return: None
        """
        while self._runner.poll() is Execute.PROCESS_NOT_STARTED:
            sleep(constants.BUSY_WAIT_INTERVAL)
        if self._runner.poll() is not Execute.PROCESS_RUNNING:
            self.error = True

    def _mount_command(self, image, fs, mountpoint):
        command = ['imagemount', '-f', image, '-d', self.device, '-m', mountpoint, '-t', fs, '-r', '-D']
        self._runner = Execute(command)
        self._runner.run()
        status = self._runner.poll()
        if not self._unmounting and status != 0:
            raise MountException("Mounting returned exit code different than 0")

    def _exception_callback(self, source, e):
        self.error = True

    def unmount(self):
        """
        Unmounts the previously mounted image file.
        :return: None
        """
        if self._thread.isAlive():
            self._unmounting = True
            self._runner.kill()
            self._thread.join()
            self._unmounting = False
        command = ['umount', self.mountpoint]
        Execute(command).run()

    def reset(self):
        """
        Resets the node so that it can be used again for mounting another image.
        :return: None
        """
        self.unmount()
        self._runner = None
        self._thread = None
        self.error = False


class _NBDPool:
    """
    This class implements the pool design pattern for provision of the NBDNodes.
    """
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
        """
        Reserves and returns a single NBDNode object.
        :return: NBDNode object.
        :exception: Exception will be raised if no more NBDNodes are available in the pool.
        """
        with self._lock:
            try:
                node = self._nbd_nodes.pop()
                self._used_nodes.append(node)
                return node
            except IndexError:
                raise MountException("Not enough resources to mount all partitions of " +
                                     "this backup. Unmount other backups and try again.")

    def release(self, node):
        """
        Returns a NBDNode object into the pool of available nodes.
        :param node: NBDNode object to be released.
        :return: None
        """
        with self._lock:
            try:
                self._used_nodes.remove(node)
                node.reset()
                self._nbd_nodes.append(node)
            except:
                raise

# initialise singleton
NBDPool = _NBDPool()
