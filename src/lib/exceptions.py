
class DiskImageException(BaseException):
    pass


class DetectionException(DiskImageException):
    pass


class BackupOperationException(DiskImageException):
    pass


class ImageException(DiskImageException):
    """Raised in case of backup and restoration issues."""
    pass


class DiskSpaceException(DiskImageException):
    pass