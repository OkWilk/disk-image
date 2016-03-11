import constants
from os import path, mkdir, remove
from shutil import rmtree
from .runcommand import Execute


class SquashfsWrapper:
    # TODO: add error handling
    SQFS_MNT_DIR = 'sqfs_mnt'

    def __init__(self, backupset):
        self.backupset = backupset
        self.mounted = False
        self._backup_dir = constants.BACKUP_PATH + self.backupset.id + '/'
        self._mnt_dir = self._backup_dir + self.SQFS_MNT_DIR + '/'

    def mount(self):
        if path.exists(self._backup_dir):
            try:
                mkdir(self._mnt_dir)
            except FileExistsError:
                pass
            for partition in self.backupset.partitions:
                self._image_prefix = 'part' + str(partition.id)
                self._create_mnt_dir(partition)
                self._mount_image(partition)
                self._create_symlink(partition)
            self.mounted = True
        else:
            Exception('Cannot open backup directory.')

    def umount(self):
        for partition in self.backupset.partitions:
            self._image_prefix = 'part' + str(partition.id)
            self._remove_symlink(partition)
        self._umount_images()
        self._remove_mnt_dir()
        self.mounted = False

    def _create_mnt_dir(self, partition):
        try:
            mkdir(self._mnt_dir + self._image_prefix)
        except FileExistsError:
            pass

    def _mount_image(self, partition):
        sqfs_file = self._backup_dir + self._image_prefix + '.sqfs'
        mnt_path = self._mnt_dir + self._image_prefix
        command = ['mount', sqfs_file, mnt_path]
        Execute(command).run()

    def _create_symlink(self, partition):
            symlink_file = self._backup_dir + self._image_prefix + '.img'
            image_file = self._mnt_dir + self._image_prefix + '/' + self._image_prefix + '.img'
            command = ['ln', '-s', image_file, symlink_file]
            Execute(command).run()

    def _remove_symlink(self, partition):
        symlink_file = self._backup_dir + self._image_prefix + '.img'
        if path.exists(symlink_file) and path.islink(symlink_file):
            remove(symlink_file)

    def _umount_images(self):
        path = self._mnt_dir + '*'
        command = 'umount ' + path
        Execute(command, shell=True).run()

    def _remove_mnt_dir(self):
        if path.exists(self._mnt_dir):
            rmtree(self._mnt_dir)
