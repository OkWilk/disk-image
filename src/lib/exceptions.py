"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

class DiskImageException(Exception):
    pass


class BackupsetException(DiskImageException):
    pass


class IllegalOperationException(DiskImageException):
    pass


class DetectionException(DiskImageException):
    pass


class BackupOperationException(DiskImageException):
    pass


class ImageException(DiskImageException):
    pass


class DiskSpaceException(DiskImageException):
    pass


class MountException(DiskImageException):
    pass