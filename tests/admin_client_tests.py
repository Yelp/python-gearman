import unittest
import collections

import sys
sys.path.insert(0, '/nail/home/mtai/pg/python-gearman')

from gearman.admin_client import GearmanAdminClient
from gearman.admin_client_handler import GearmanAdminClientCommandHandler

from gearman.errors import ServerUnavailable, InvalidAdminClientState, ProtocolError
from gearman.protocol import GEARMAN_COMMAND_TEXT_COMMAND, \
    GEARMAN_SERVER_COMMAND_STATUS, GEARMAN_SERVER_COMMAND_VERSION, GEARMAN_SERVER_COMMAND_WORKERS, GEARMAN_SERVER_COMMAND_MAXQUEUE, GEARMAN_SERVER_COMMAND_SHUTDOWN

from tests._core_testing import _GearmanAbstractTest, MockGearmanConnectionManager, MockGearmanConnection

class MockGearmanAdminClient(MockGearmanConnectionManager, GearmanAdminClient):
    def __init__(self, *largs, **kwargs):
        kwargs.setdefault('_is_testing_', True)
        super(MockGearmanAdminClient, self).__init__(*largs, **kwargs)

class CommandHandlerStateMachineTest(_GearmanAbstractTest):
    """Test the public interface a GearmanWorker may need to call in order to update state on a GearmanWorkerCommandHandler"""
    connection_manager_class = MockGearmanAdminClient
    command_handler_class = GearmanAdminClientCommandHandler

    def setUp(self):
        super(CommandHandlerStateMachineTest, self).setUp()
        self.connection_manager.current_connection = self.connection
        self.connection_manager.current_handler = self.command_handler

    def test_send_illegal_server_commands(self):
        self.assertRaises(ProtocolError, self.send_server_command, "This is not a server command")

    def test_state_and_protocol_errors_for_status(self):
        self.send_server_command(GEARMAN_SERVER_COMMAND_STATUS)

        # Test premature popping as this we aren't until ready we see the '.'
        self.assertRaises(InvalidAdminClientState, self.pop_response, GEARMAN_SERVER_COMMAND_STATUS)

        # Test malformed server status
        self.assertRaises(ProtocolError, self.recv_server_response, '\t'.join(['12', 'IP-A', 'CLIENT-A']))

        self.recv_server_response('.')

        server_response = self.pop_response(GEARMAN_SERVER_COMMAND_STATUS)
        self.assertEquals(server_response, tuple())

    def test_multiple_status(self):
        self.send_server_command(GEARMAN_SERVER_COMMAND_STATUS)
        self.recv_server_response('\t'.join(['test_function', '1', '5', '17']))
        self.recv_server_response('\t'.join(['another_function', '2', '4', '23']))
        self.recv_server_response('.')

        server_response = self.pop_response(GEARMAN_SERVER_COMMAND_STATUS)
        self.assertEquals(len(server_response), 2)

        test_response, another_response = server_response
        self.assertEquals(test_response['function_name'], 'test_function')
        self.assertEquals(test_response['queued'], 1)
        self.assertEquals(test_response['running'], 5)
        self.assertEquals(test_response['workers'],  17)

        self.assertEquals(another_response['function_name'], 'another_function')
        self.assertEquals(another_response['queued'], 2)
        self.assertEquals(another_response['running'], 4)
        self.assertEquals(another_response['workers'],  23)

    def test_version(self):
        expected_version = '0.12345'

        self.send_server_command(GEARMAN_SERVER_COMMAND_VERSION)
        self.recv_server_response(expected_version)

        server_response = self.pop_response(GEARMAN_SERVER_COMMAND_VERSION)
        self.assertEquals(expected_version, server_response)

    def test_state_and_protocol_errors_for_workers(self):
        self.send_server_command(GEARMAN_SERVER_COMMAND_WORKERS)

        # Test premature popping as this we aren't until ready we see the '.'
        self.assertRaises(InvalidAdminClientState, self.pop_response, GEARMAN_SERVER_COMMAND_WORKERS)

        # Test malformed responses
        self.assertRaises(ProtocolError, self.recv_server_response, ' '.join(['12', 'IP-A', 'CLIENT-A']))
        self.assertRaises(ProtocolError, self.recv_server_response, ' '.join(['12', 'IP-A', 'CLIENT-A', 'NOT:']))

        self.recv_server_response('.')

        server_response = self.pop_response(GEARMAN_SERVER_COMMAND_WORKERS)
        self.assertEquals(server_response, tuple())

    def test_multiple_workers(self):
        self.send_server_command(GEARMAN_SERVER_COMMAND_WORKERS)
        self.recv_server_response(' '.join(['12', 'IP-A', 'CLIENT-A', ':', 'function-A', 'function-B']))
        self.recv_server_response(' '.join(['13', 'IP-B', 'CLIENT-B', ':', 'function-C']))
        self.recv_server_response('.')

        server_response = self.pop_response(GEARMAN_SERVER_COMMAND_WORKERS)
        self.assertEquals(len(server_response), 2)

        test_response, another_response = server_response
        self.assertEquals(test_response['file_descriptor'], '12')
        self.assertEquals(test_response['ip'], 'IP-A')
        self.assertEquals(test_response['client_id'], 'CLIENT-A')
        self.assertEquals(test_response['function_names'],  ('function-A', 'function-B'))

        self.assertEquals(another_response['file_descriptor'], '13')
        self.assertEquals(another_response['ip'], 'IP-B')
        self.assertEquals(another_response['client_id'], 'CLIENT-B')
        self.assertEquals(another_response['function_names'],  ('function-C', ))

    def test_maxqueue(self):
        self.send_server_command(GEARMAN_SERVER_COMMAND_MAXQUEUE)
        self.assertRaises(ProtocolError, self.recv_server_response, 'NOT OK')

        # Pop prematurely
        self.assertRaises(InvalidAdminClientState, self.pop_response, GEARMAN_SERVER_COMMAND_MAXQUEUE)

        self.recv_server_response('OK')
        server_response = self.pop_response(GEARMAN_SERVER_COMMAND_MAXQUEUE)
        self.assertEquals(server_response, 'OK')

    def test_shutdown(self):
        self.send_server_command(GEARMAN_SERVER_COMMAND_SHUTDOWN)

        # Pop prematurely
        self.assertRaises(InvalidAdminClientState, self.pop_response, GEARMAN_SERVER_COMMAND_SHUTDOWN)

        self.recv_server_response(None)
        server_response = self.pop_response(GEARMAN_SERVER_COMMAND_SHUTDOWN)
        self.assertEquals(server_response, None)

    def send_server_command(self, expected_command):
        self.command_handler.send_text_command(expected_command)
        expected_line = "%s\n" % expected_command
        self.assert_sent_command(GEARMAN_COMMAND_TEXT_COMMAND, raw_text=expected_line)

        self.assertEqual(self.command_handler._sent_commands[0], expected_command)

    def recv_server_response(self, response_line):
        self.command_handler.recv_command(GEARMAN_COMMAND_TEXT_COMMAND, raw_text=response_line)

    def pop_response(self, expected_command):
        server_cmd, server_response = self.command_handler.pop_response()
        self.assertEquals(expected_command, server_cmd)

        return server_response

if __name__ == '__main__':
    unittest.main()
