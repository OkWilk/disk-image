import unittest
from errno import EIO
from unittest.mock import Mock, patch, mock_open
from threading import Thread
from src.core.runcommand import Execute, OutputParser, OutputToFileConverter
from time import sleep


class RunCommandTest(unittest.TestCase):

    def test_create_default_execute_object(self):
        execute = Execute(['echo'])
        self.assertEqual(['echo'], execute.command)
        self.assertTrue(execute.output_parser)
        self.assertFalse(execute.shell)
        self.assertFalse(execute.use_pty)

    def test_create_execute_object_with_shell_enabled(self):
        execute = Execute('echo', shell=True)
        self.assertEqual('echo', execute.command)
        self.assertTrue(execute.shell)

    def test_create_execute_object_with_pty_enabled(self):
        execute = Execute(['echo'], use_pty=True)
        self.assertTrue(execute.use_pty)

    def test_execute_without_pty(self):
        execute = Execute(['echo', 'helpers'])
        execute.run()
        self.assertTrue('helpers' in str(execute.output()))

    def test_execute_with_pty(self):
        execute = Execute('python3 ./helpers/listprint.py 5 test test2',
                          shell=True, use_pty=True)
        t = Thread(target=execute.run)
        t.start()
        sleep(0.3)
        self.assertTrue(execute.output())

    def test_kill_unstarted_process(self):
        execute = Execute(['echo'])
        self.assertTrue(execute.kill() == None)

    def test_kill_running_process(self):
        execute = Execute(['tail', '-f', '/dev/null'])
        t = Thread(target=execute.run)
        t.start()
        while execute.process is None:
            sleep(0.1)
        self.assertTrue(execute.poll() == None)
        execute.kill()
        self.assertTrue(execute.poll())

    def test_parser_is_called_to_generate_output(self):
        parser = Mock()
        execute = Execute(['echo', 'test'], parser)
        execute.run()
        parser.parse.assert_called_with('test\n')

    def test_poll_process(self):
        execute = Execute(['echo'])
        self.assertEqual(-1, execute.poll())  # process never started
        execute.process = Mock()
        execute.process.poll.return_value = None
        self.assertEqual(None, execute.poll())  # process still running
        execute.process.poll.return_value = 0
        self.assertEqual(0, execute.poll())  # process finished

    @patch('src.core.runcommand.os')
    def test_EIO_do_not_raise_exception(self, mock_os):
        execute = Execute(['echo'], use_pty=True)
        mock_os.read.side_effect = IOError(EIO, None)
        execute.run()
        self.assertTrue(mock_os.read.called)

    @patch('src.core.runcommand.os')
    def test_non_EIO_raises_exception(self, mock_os):
        execute = Execute(['echo'], use_pty=True)
        mock_os.read.side_effect = IOError("test")
        with self.assertRaises(IOError):
            execute.run()

    @patch('src.core.runcommand.os')
    def test_exit_process_when_no_more_output_is_generated(self, mock_os):
        mock_os.read.return_value = ""
        execute = Execute(['echo'], use_pty=True)
        execute.run()
        self.assertTrue(mock_os.close.called)
        self.assertNotEqual(None, execute.poll())


class OutputParserTest(unittest.TestCase):

    def test_parser_saves_output_to_variable(self):
        parser = OutputParser()
        test_str = 'test string!'
        parser.parse(test_str)
        self.assertEqual(test_str, parser.output)


class OutputToFileConverterTest(unittest.TestCase):

    def test_converter_sets_append_correctly(self):
        converter = OutputToFileConverter('file.txt')
        self.assertEqual('w', converter.mode)
        converter = OutputToFileConverter('file.txt', append=True)
        self.assertEqual('a', converter.mode)
        self.assertEqual('file.txt', converter.file)

    def test_output_is_written_to_file(self):
        converter = OutputToFileConverter('file.txt')
        data = 'some data'
        mock = mock_open()
        with patch('src.core.runcommand.open', mock, create=True):
            converter.parse(data)
        self.assertEqual(data, converter.output)
        handle = mock()
        handle.write.assert_called_with(data)

if __name__ == '__main__':
    unittest.main()
