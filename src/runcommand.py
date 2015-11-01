""" 
Source of the initial code: 
http://stackoverflow.com/questions/11165521/using-subprocess-with-select-and-pty-hangs-when-capturing-output 
Refactored by: Oktawiusz Wilk
"""

import errno
import os
import pty
import subprocess

class OutputParser:
    def __init__(self):
        self.output = None
    
    def parse(self, data):
        self.output = data

class Execute:
    """ 
    Executes Unix command forcing a line-buffering and redirecting each line from stdout to output_filter function
    Warning: This function works only on Linux
    """

    def __init__(self, command:list, output_parser:'OutputParser'=OutputParser(), use_pty:bool=False, shell:bool=False, buffer_size:int=1024):
        self.command = command
        self.output_parser = output_parser
        self.use_pty = use_pty
        self.shell = shell
        self.buffer_size = buffer_size
        self.process = None
        
    def run(self):
        if self.use_pty:
            return self._run_with_pty()
        else:
            return self._run_without_pty()
    
    def kill(self):
        if self.process:
            if self.process.poll() is None:
                self.process.kill()
            self.process.wait()
            return self.process.poll()
        else:
            return None

    def poll(self):
        """ Returns the error code of the finished process, None if process is still running, and -1 if process was not started yet. """
        if self.process:
            return self.process.poll()
        else:
            return -1
        
    def output(self):
        return self.output_parser.output
    
    def _run_with_pty(self):
        master_fd, slave_fd = pty.openpty() 
        self.process = subprocess.Popen(self.command, stdin=slave_fd, stdout=slave_fd, stderr=subprocess.STDOUT, close_fds=True, shell=self.shell)
        os.close(slave_fd)
        try:
            while True:
                try:
                    data = os.read(master_fd, self.buffer_size)
                except OSError as e:
                    if e.errno == errno.EIO:
                        break # EIO == EOF on some systems
                    raise
                else:
                    if not data: # EOF
                        break
                    self.output_parser.parse(data.decode("utf-8"))
        finally:
            os.close(master_fd)
            self.kill()
    
    def _run_without_pty(self):
        self.process = subprocess.Popen(self.command, stdout=subprocess.PIPE, shell=self.shell)
        out, err = self.process.communicate()
        self.output_parser.parse(out)            
        return self.kill()