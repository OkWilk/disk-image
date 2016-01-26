from threading import Thread


class ExtendedThread(Thread):

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