import collections
import random
import unittest

from gearman._command_handler import GearmanCommandHandler
from gearman._connection import GearmanConnection
from gearman._connection_manager import GearmanConnectionManager

from gearman.constants import BACKGROUND_JOB, FOREGROUND_JOB, NO_PRIORITY, HIGH_PRIORITY, LOW_PRIORITY
from gearman.errors import ConnectionError
from gearman.job import GearmanJob, GearmanJobRequest, GEARMAN_JOB_STATE_QUEUED
from gearman.protocol import get_command_name

class MockGearmanConnection(GearmanConnection):
    def __init__(self, *largs, **kwargs):
        kwargs.setdefault('hostname', None)
        super(MockGearmanConnection, self).__init__(*largs, **kwargs)

        self._is_connected = True
        self._should_fail_on_connect = False

    def connect(self):
        super(MockGearmanConnection, self).connect()
        if self._should_fail_on_connect:
            raise ConnectionError('Mock connection failure')

    def bind_client_socket(self):
        pass

    def read_data_from_socket(self):
        pass

    def send_data_to_socket(self):
        pass

    def __repr__(self):
        return ('<GearmanConnection %s:%d connected=%s> (%s)' %
            (self.gearman_host, self.gearman_port, self._is_connected, id(self)))

class MockGearmanConnectionManager(GearmanConnectionManager):
    """Handy mock client base to test Worker/Client/Abstract ClientBases"""
    def poll_connections_once(self, connections, timeout=None):
        return True

    def handle_error(self, gearman_connection):
        pass


class _GearmanAbstractTest(unittest.TestCase):
    connection_class = MockGearmanConnection
    connection_manager_class = MockGearmanConnectionManager
    command_handler_class = None

    def setUp(self):
        self.connection = self.connection_class(hostname=None)

        # Create a new MockGearmanTestClient on the fly
        testing_attributes = {'command_handler_class': self.command_handler_class, 'connection_class': self.connection_class}
        testing_client_class = type('MockGearmanTestingClient', (self.connection_manager_class, ), testing_attributes)

        self.connection_manager = testing_client_class()
        self.connection_manager._add_connection(self.connection)

        self.command_handler = self.connection_manager.connection_to_handler_map[self.connection]

    def generate_job(self):
        return GearmanJob(self.connection, handle=str(random.random()), function_name='test_function_name', unique=str(random.random()), data=str(random.random()))

    def generate_job_dict(self):
        current_job = self.generate_job()
        return current_job.to_dict()

    def generate_job_request(self, priority=NO_PRIORITY, background=FOREGROUND_JOB):
        job_handle = str(random.random())
        current_job = GearmanJob(conn=self.connection, handle=job_handle, function_name='client_echo', unique=str(random.random()), data=str(random.random()))
        current_request = GearmanJobRequest(current_job, initial_priority=priority, background=background)

         # Start this off as someone being queued
        current_request.state = GEARMAN_JOB_STATE_QUEUED

        return current_request

    def assert_jobs_equal(self, job_actual, job_expected):
        # Validates that GearmanJobs are essentially equal
        self.assertEqual(job_actual.handle, job_expected.handle)
        self.assertEqual(job_actual.func, job_expected.func)
        self.assertEqual(job_actual.unique, job_expected.unique)
        self.assertEqual(job_actual.data, job_expected.data)

    def assert_sent_command(self, expected_cmd_type, **expected_cmd_args):
        # Make sure any commands we're passing through the CommandHandler gets properly passed through to the client base
        client_cmd_type, client_cmd_args = self.connection._outgoing_commands.popleft()
        self.assert_commands_equal(client_cmd_type, expected_cmd_type)
        self.assertEqual(client_cmd_args, expected_cmd_args)

    def assert_no_pending_commands(self):
        self.assertEqual(self.connection._outgoing_commands, collections.deque())

    def assert_commands_equal(self, cmd_type_actual, cmd_type_expected):
        self.assertEqual(get_command_name(cmd_type_actual), get_command_name(cmd_type_actual))
