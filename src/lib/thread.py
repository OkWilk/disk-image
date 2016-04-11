"""
Author:     Oktawiusz Wilk
Date:       10/04/2016
License:    GPL
"""

from threading import Thread


class ExtendedThread(Thread):
    """
    This class wraps the standard Thread from the Python threading library to add a callback
    function in case of exception being raised on the thread. With the callback method is provided
    the thread can pass the exception object to the parent thread to notify it about the error.
    """

    def __init__(self, exception_callback=None, *args, **kwargs):
        self._callback = exception_callback
        super().__init__(*args, **kwargs)

    def run(self):
        try:
            if self._target:
                self._target(*self._args, **self._kwargs)
        except BaseException as e:
            if self._callback:
                self._callback(self, e)
            else:
                raise e
        finally:
            del self._target, self._args, self._kwargs, self._callback


class ThreadException(BaseException):
    pass