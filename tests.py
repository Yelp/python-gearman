import logging
import os, sys, signal, threading
import unittest, time
import collections
import random
import types

from gearman._client_base import GearmanConnectionHandler, GearmanClientBase
from gearman.worker import GearmanWorkerConnectionHandler, GearmanWorker
from gearman.client import GearmanClientConnectionHandler, GearmanClient
from gearman.manager import GearmanManagerConnectionHandler, GearmanManager

from gearman.connection import GearmanConnection
from gearman.constants import BACKGROUND_JOB
from gearman.errors import ConnectionError, ServerUnavailable, InvalidClientState
from gearman.job import GearmanJob, GearmanJobRequest, GEARMAN_JOB_STATE_PENDING, GEARMAN_JOB_STATE_QUEUED, GEARMAN_JOB_STATE_FAILED, GEARMAN_JOB_STATE_COMPLETE
from gearman.protocol import *


job_servers = ['127.0.0.1']

class MockGearmanConnection(GearmanConnection):
    def __init__(self, *largs, **kwargs):
        kwargs.setdefault('hostname', None)
        super(MockGearmanConnection, self).__init__(*largs, **kwargs)

        self._is_connected = None
        self._should_fail_on_connect = False

    def connect(self):
        if self._should_fail_on_connect:
            raise ConnectionError('Mock connection failure')

        self._is_connected = bool(self._is_connected is None) or self._is_connected

    def send_data_from_buffer(self):
        pass

    def __repr__(self):
        return ('<GearmanConnection %s:%d connected=%s> (%s)' %
            (self.gearman_host, self.gearman_port, self._is_connected, id(self)))

class MockGearmanClientBase(GearmanClientBase):
    """Handy mock client base to test Worker/Client/Abstract ClientBases"""
    def __init__(self, gearman_connection_handler_class, gearman_connection_class):
        super(MockGearmanClientBase, self).__init__(gearman_connection_handler_class=gearman_connection_handler_class, gearman_connection_class=gearman_connection_class)
        # ConnectionHandler to command queue map
        self.command_queues = collections.defaultdict(collections.deque)

        # ConnectionHandler to job queue maps
        self.worker_job_queues = collections.defaultdict(collections.deque)

    def poll_connections_once(self, connections, timeout=None):
        return True

    # Catch all IO here in the test_command_queue
    def send_command(self, gearman_connection, cmd_type, cmd_args):
        actual_connection_handler = self.connection_handlers[gearman_connection]
        self.command_queues[actual_connection_handler].append((cmd_type, cmd_args))

    def on_job_execute(self, current_job):
        """Masquerade as a GearmanWorker"""
        connection_handler = self.connection_handlers[current_job.conn]
        self.worker_job_queues[connection_handler].append(current_job)

    def handle_error(self, gearman_connection):
        pass

class MockGearmanConnectionHandler(GearmanConnectionHandler):
    """Handy mock client base to test Worker/Client/Abstract ConnectionHandlers"""
    def __init__(self, *largs, **kwargs):
        super(MockGearmanConnectionHandler, self).__init__(*largs, **kwargs)
        self.sent_command_queue = collections.deque()
        self.recv_command_queue = collections.deque()

    def send_command(self, cmd_type, **cmd_args):
        # Catch all outbound commands from this connection handler
        self.sent_command_queue.append((cmd_type, cmd_args))
        sent_result = super(MockGearmanConnectionHandler, self).send_command(cmd_type, **cmd_args)
        return sent_result

    def recv_command(self, cmd_type, **cmd_args):
        # Catch all outbound commands from this connection handler
        self.recv_command_queue.append((cmd_type, cmd_args)) 
        recv_result = super(MockGearmanConnectionHandler, self).recv_command(cmd_type, **cmd_args)
        return recv_result

    def recv_noop(self):
        pass


# class GearmanConnectionTest(_GearmanAbstractTest):
#     def setUp(self):
#         self.connection = GearmanConnection(job_servers[0], blocking_timeout=2.0)
#         self.connection.connect()
# 
#     def test_no_args(self):
#         self.connection.send_command(GEARMAN_COMMAND_ECHO_REQ, dict(text=''))
#         cmd_tuple = self.connection.recv_command()
#         self.failUnless(cmd_tuple)
# 
#         cmd_type, cmd_args = cmd_tuple
#         self.assertEqual(cmd_type, GEARMAN_COMMAND_ECHO_RES)
# 
#     def test_with_args(self):
#         self.connection.send_command(GEARMAN_COMMAND_SUBMIT_JOB, dict(function_name='echo', unique='%s' % time.time(), data='tea'))
#         cmd_tuple = self.connection.recv_command()
#         self.failUnless(cmd_tuple)
# 
#         cmd_type, cmd_args = cmd_tuple
#         self.assertEqual(cmd_type, GEARMAN_COMMAND_JOB_CREATED)

class _GearmanAbstractTest(unittest.TestCase):
    connection_class = MockGearmanConnection
    client_base_class = MockGearmanClientBase
    connection_handler_class = MockGearmanConnectionHandler

    def setUp(self):
        self.connection = self.connection_class(hostname=None)

        self.client_base = self.client_base_class(gearman_connection_handler_class=self.connection_handler_class, gearman_connection_class=self.connection_class)
        self.client_base._add_connection(self.connection)

        self.connection_handler = self.client_base.connection_handlers[self.connection]

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
        # Make sure any commands we're passing through the ConnectionHandler gets properly passed through to the client base
        client_cmd_type, client_cmd_args = self.client_base.command_queues[self.connection_handler].popleft()
        self.assert_commands_equal(client_cmd_type, expected_cmd_type)
        self.assertEqual(client_cmd_args, expected_cmd_args)

    def assert_no_pending_commands(self):
        self.assertEqual(self.client_base.command_queues[self.connection_handler], collections.deque())

    def assert_commands_equal(self, cmd_type_actual, cmd_type_expected):
        self.assertEqual(get_command_name(cmd_type_actual), get_command_name(cmd_type_actual))


class GearmanClientTest(_GearmanAbstractTest):
    """Test the public client interface"""
    client_base_class = GearmanClient
    connection_handler_class = GearmanClientConnectionHandler

    def setUp(self):
        super(GearmanClientTest, self).setUp()
        self.original_poll_connections_once = self.client_base.poll_connections_once

    def tearDown(self):
        super(GearmanClientTest, self).tearDown()
        self.client_base.poll_connections_once = self.original_poll_connections_once

    def generate_job_request(self):
        current_request = super(GearmanClientTest, self).generate_job_request()

        job_handle = current_request.get_handle()
        self.connection_handler.handle_to_request_map[job_handle] = current_request
        return current_request

    def test_connection_rotation_for_requests(self):
        self.client_base.connection_list = []
        self.client_base.connection_handlers = {}

        # Spin up a bunch of imaginary gearman connections
        failed_connection = MockGearmanConnection()
        failed_connection._should_fail_on_connect = True
        failed_connection._is_connected = False

        failed_then_retried_connection = MockGearmanConnection()
        failed_then_retried_connection._should_fail_on_connect = True
        failed_then_retried_connection._is_connected = True

        good_connection = MockGearmanConnection()
        good_connection._should_fail_on_connect = False
        good_connection._is_connected = True

        # Register all our connections
        self.client_base._add_connection(failed_connection)
        self.client_base._add_connection(failed_then_retried_connection)
        self.client_base._add_connection(good_connection)

        current_request = self.generate_job_request()

        self.failIf(current_request in self.client_base.request_to_rotating_connection_queue)

        # Make sure that when we start up, we get our good connection
        chosen_conn = self.client_base.choose_connection_for_request(current_request)
        self.assertEqual(chosen_conn, good_connection)

        # No state changed so we should still go there
        chosen_conn = self.client_base.choose_connection_for_request(current_request)
        self.assertEqual(chosen_conn, good_connection)

        # Pretend like our good connection died so we'll need to choose somethign else
        good_connection._should_fail_on_connect = True
        good_connection._is_connected = False

        failed_then_retried_connection._should_fail_on_connect = False
        failed_then_retried_connection._is_connected = True

        # Make sure we rotate good_connection and failed_connection out
        chosen_conn = self.client_base.choose_connection_for_request(current_request)
        self.assertEqual(chosen_conn, failed_then_retried_connection)

    def test_no_connections_for_rotation_for_requests(self):
        self.client_base.connection_list = []
        self.client_base.connection_handlers = {}

        current_request = self.generate_job_request()

        # No connections == death
        self.assertRaises(ServerUnavailable, self.client_base.choose_connection_for_request, current_request)

        # Spin up a bunch of imaginary gearman connections
        failed_connection = MockGearmanConnection()
        failed_connection._should_fail_on_connect = True
        failed_connection._is_connected = False
        self.client_base._add_connection(failed_connection)

        # All failed connections == death
        self.assertRaises(ServerUnavailable, self.client_base.choose_connection_for_request, current_request)

    def test_multiple_fg_job_submission(self):
        submitted_job_count = 5
        expected_job_list = [self.generate_job() for _ in xrange(submitted_job_count)]
        def mark_jobs_created(connections, timeout=None):
            for current_job in expected_job_list:
                self.connection_handler.recv_command(GEARMAN_COMMAND_JOB_CREATED, job_handle=current_job.handle)

        self.client_base.poll_connections_once = mark_jobs_created

        job_dictionaries = [current_job.to_dict() for current_job in expected_job_list]

        # Test multiple job submission
        job_requests = self.client_base.submit_multiple_jobs(job_dictionaries, background=FOREGROUND_JOB)
        for current_request, expected_job in zip(job_requests, expected_job_list):
            current_job = current_request.get_job()
            self.assert_jobs_equal(current_job, expected_job)

            self.assertEqual(current_request.priority, NO_PRIORITY)
            self.assertEqual(current_request.background, FOREGROUND_JOB)
            self.assertEqual(current_request.state, GEARMAN_JOB_STATE_QUEUED)

            self.assertFalse(current_request.is_complete())

    def test_single_bg_job_submission(self):
        expected_job = self.generate_job()
        def mark_job_created(connections, timeout=None):
            self.connection_handler.recv_command(GEARMAN_COMMAND_JOB_CREATED, job_handle=expected_job.handle)

        self.client_base.poll_connections_once = mark_job_created
        job_request = self.client_base.submit_job(expected_job.func, expected_job.data, unique=expected_job.unique, background=BACKGROUND_JOB, priority=LOW_PRIORITY)

        current_job = job_request.get_job()
        self.assert_jobs_equal(current_job, expected_job)

        self.assertEqual(job_request.priority, LOW_PRIORITY)
        self.assertEqual(job_request.background, BACKGROUND_JOB)
        self.assertEqual(job_request.state, GEARMAN_JOB_STATE_QUEUED)

        self.assertTrue(job_request.is_complete())

    def test_single_fg_job_submission_timeout(self):
        expected_job = self.generate_job()
        def job_failed_submission(connections, timeout=None):
            pass

        self.client_base.poll_connections_once = job_failed_submission
        job_request = self.client_base.submit_job(expected_job.func, expected_job.data, unique=expected_job.unique, priority=HIGH_PRIORITY, timeout=0.1)

        self.assertEqual(job_request.priority, HIGH_PRIORITY)
        self.assertEqual(job_request.background, FOREGROUND_JOB)
        self.assertEqual(job_request.state, GEARMAN_JOB_STATE_PENDING)

        self.assertFalse(job_request.is_complete())
        self.assertTrue(job_request.timed_out)

    def test_wait_for_multiple_jobs_to_complete_or_timeout(self):
        completed_request = self.generate_job_request()
        failed_request = self.generate_job_request()
        timeout_request = self.generate_job_request()

        completed_request.state = GEARMAN_JOB_STATE_QUEUED
        failed_request.state = GEARMAN_JOB_STATE_QUEUED
        timeout_request.state = GEARMAN_JOB_STATE_QUEUED

        self.update_requests = True
        def multiple_job_updates(connections, timeout=None):
            # Only give a single status update and have the 3rd job handle timeout
            if self.update_requests:
                self.connection_handler.recv_command(GEARMAN_COMMAND_WORK_COMPLETE, job_handle=completed_request.get_handle(), data='12345')
                self.connection_handler.recv_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=failed_request.get_handle())
                self.update_requests = False

        self.client_base.poll_connections_once = multiple_job_updates

        finished_requests = self.client_base.wait_for_jobs_to_complete([completed_request, failed_request, timeout_request], timeout=0.1)
        del self.update_requests

        finished_completed_request, finished_failed_request, finished_timeout_request = finished_requests
        self.assert_jobs_equal(finished_completed_request.get_job(), completed_request.get_job())
        self.assertEqual(finished_completed_request.state, GEARMAN_JOB_STATE_COMPLETE)
        self.assertEqual(finished_completed_request.result, '12345')
        self.assertFalse(finished_completed_request.timed_out)

        self.assert_jobs_equal(finished_failed_request.get_job(), failed_request.get_job())
        self.assertEqual(finished_failed_request.state, GEARMAN_JOB_STATE_FAILED)
        self.assertEqual(finished_failed_request.result, None)
        self.assertFalse(finished_failed_request.timed_out)

        self.assertEqual(finished_timeout_request.state, GEARMAN_JOB_STATE_QUEUED)
        self.assertEqual(finished_timeout_request.result, None)
        self.assertTrue(finished_timeout_request.timed_out)

    def test_wait_for_single_job_completion(self):
        single_request = self.generate_job_request()
        single_request.state = GEARMAN_JOB_STATE_QUEUED

        def complete_job(connections, timeout=None):
            self.connection_handler.recv_command(GEARMAN_COMMAND_WORK_COMPLETE, job_handle=single_request.get_handle(), data='12345')

        self.client_base.poll_connections_once = complete_job

        finished_request = self.client_base.wait_for_job_completion(single_request)
        self.assertEqual(finished_request.state, GEARMAN_JOB_STATE_COMPLETE)
        self.assertEqual(finished_request.result, '12345')
        self.assertFalse(finished_request.timed_out)

    def test_get_status(self):
        self.connection.connect()

        single_request = self.generate_job_request()
        single_request.state = GEARMAN_JOB_STATE_QUEUED

        def retrieve_status(connections, timeout=None):
            self.connection_handler.recv_command(GEARMAN_COMMAND_STATUS_RES, job_handle=single_request.get_handle(), known='1', running='0', numerator='0.0', denominator='1.0')

        self.client_base.poll_connections_once = retrieve_status

        request_status = self.client_base.get_status(single_request)
        self.failUnless(request_status)
        self.assertTrue(request_status['known'])
        self.assertFalse(request_status['running'])
        self.assertEqual(request_status['numerator'], 0.0)
        self.assertEqual(request_status['denominator'], 1.0)
        self.assertFalse(request_status['timed_out'])

    def test_get_status_timeout(self):
        self.connection.connect()

        single_request = self.generate_job_request()
        single_request.state = GEARMAN_JOB_STATE_QUEUED

        def retrieve_status_timeout(connections, timeout=None):
            pass

        self.client_base.poll_connections_once = retrieve_status_timeout

        request_status = self.client_base.get_status(single_request, timeout=0.1)
        self.assertTrue(request_status['timed_out'])


class GearmanWorkerTest(_GearmanAbstractTest):
    """Test the public worker interface"""
    client_base_class = GearmanWorker
    connection_handler_class = GearmanWorkerConnectionHandler

    def test_registering_functions(self):
        # Tests that the abilities were set on the GearmanWorker AND the GearmanWorkerConnectionHandler
        # Does NOT test that commands were actually sent out as that is tested in GearmanWorkerConnectionHandlerInterfaceTest.test_set_abilities
        def fake_callback_one(worker_connection_handler, current_job):
            pass

        def fake_callback_two(worker_connection_handler, current_job):
            pass

        # Register a single callback
        self.client_base.register_function('fake_callback_one', fake_callback_one)
        self.failUnless('fake_callback_one' in self.client_base.worker_abilities)
        self.failIf('fake_callback_two' in self.client_base.worker_abilities)
        self.assertEqual(self.client_base.worker_abilities['fake_callback_one'], fake_callback_one)
        self.assertEqual(self.connection_handler._connection_abilities, ['fake_callback_one'])

        # Register another callback and make sure the connection_handler sees the same functions
        self.client_base.register_function('fake_callback_two', fake_callback_two)
        self.failUnless('fake_callback_one' in self.client_base.worker_abilities)
        self.failUnless('fake_callback_two' in self.client_base.worker_abilities)
        self.assertEqual(self.client_base.worker_abilities['fake_callback_one'], fake_callback_one)
        self.assertEqual(self.client_base.worker_abilities['fake_callback_two'], fake_callback_two)
        self.assertEqual(self.connection_handler._connection_abilities, ['fake_callback_one', 'fake_callback_two'])

        # Unregister a callback and make sure the connection_handler sees the same functions
        self.client_base.unregister_function('fake_callback_one')
        self.failIf('fake_callback_one' in self.client_base.worker_abilities)
        self.failUnless('fake_callback_two' in self.client_base.worker_abilities)
        self.assertEqual(self.client_base.worker_abilities['fake_callback_two'], fake_callback_two)
        self.assertEqual(self.connection_handler._connection_abilities, ['fake_callback_two'])

    def test_setting_client_id(self):
        new_client_id = 'HELLO'

        # Make sure nothing is set
        self.assertEqual(self.client_base.worker_client_id, None)
        self.assertEqual(self.connection_handler._client_id, None)

        self.client_base.set_client_id(new_client_id)

        # Make sure both the client and the connection handler reflect the new state
        self.assertEqual(self.client_base.worker_client_id, new_client_id)
        self.assertEqual(self.connection_handler._client_id, new_client_id)

    def test_mixed_alive_connections(self):
        self.client_base.connection_list = []
        self.client_base.connection_handlers = {}

        # Spin up a bunch of imaginary gearman connections
        good_connection = MockGearmanConnection()
        good_connection._should_fail_on_connect = False
        good_connection._is_connected = True

        failed_then_retried_connection = MockGearmanConnection()
        failed_then_retried_connection._should_fail_on_connect = True
        failed_then_retried_connection._is_connected = True

        failed_connection = MockGearmanConnection()
        failed_connection._should_fail_on_connect = True
        failed_connection._is_connected = False

        # Register all our connections
        self.client_base._add_connection(good_connection)
        self.client_base._add_connection(failed_then_retried_connection)
        self.client_base._add_connection(failed_connection)

        # The only alive connections should be the ones that ultimately be connection._is_connected
        alive_connections = self.client_base.get_alive_connections()
        self.assertTrue(good_connection in alive_connections)
        self.assertTrue(failed_then_retried_connection in alive_connections)
        self.assertFalse(failed_connection in alive_connections)

    def test_work_with_no_live_connections(self):
        self.client_base.connection_list = []
        self.client_base.connection_handlers = {}

        # We have no connections so there will never be any work to do
        self.assertRaises(ConnectionError, self.client_base.work)

        # We were started with a dead connection, make sure we bail again
        dead_connection = MockGearmanConnection()
        dead_connection._should_fail_on_connect = False
        dead_connection._is_connected = False
        self.client_base._add_connection(dead_connection)
        self.assertRaises(ConnectionError, self.client_base.work)

class GearmanConnectionHandlerTest(_GearmanAbstractTest):
    """Tests the base ConnectionHandler class that underpins all other ConnectionHandlerTests"""
    def test_recv_command(self):
        # recv_echo_res and recv_error are predefined on the ConnectionHandler
        self.connection_handler.recv_command(GEARMAN_COMMAND_NOOP)
        self.assert_recv_command(GEARMAN_COMMAND_NOOP)

        # The mock handler never implemented 'recv_all_yours' so we should get an attribute error here
        self.assertRaises(ValueError, self.connection_handler.recv_command, GEARMAN_COMMAND_ALL_YOURS)

    def test_send_command(self):
        self.connection_handler.send_command(GEARMAN_COMMAND_NOOP)
        self.assert_sent_command(GEARMAN_COMMAND_NOOP)

        # The mock handler never implemented 'recv_all_yours' so we should get an attribute error here
        self.connection_handler.send_command(GEARMAN_COMMAND_ECHO_REQ, text='hello world')
        self.assert_sent_command(GEARMAN_COMMAND_ECHO_REQ, text='hello world')

    def assert_recv_command(self, expected_cmd_type, **expected_cmd_args):
        cmd_type, cmd_args = self.connection_handler.recv_command_queue.popleft()
        self.assert_commands_equal(cmd_type, expected_cmd_type)
        self.assertEqual(cmd_args, expected_cmd_args)

    def assert_sent_command(self, expected_cmd_type, **expected_cmd_args):
        # All commands should be sent via the ConnectionHandler
        handler_cmd_type, handler_cmd_args = self.connection_handler.sent_command_queue.popleft()
        self.assert_commands_equal(handler_cmd_type, expected_cmd_type)
        self.assertEqual(handler_cmd_args, expected_cmd_args)

        super(GearmanConnectionHandlerTest, self).assert_sent_command(expected_cmd_type, **expected_cmd_args)

class GearmanClientConnectionHandlerInterfaceTest(_GearmanAbstractTest):
    """Test the public interface a GearmanClient may need to call in order to update state on a GearmanClientConnectionHandler"""
    client_base_class = MockGearmanClientBase
    connection_handler_class = GearmanClientConnectionHandler

    def test_send_job_request(self):
        current_request = self.generate_job_request()
        gearman_job = current_request.get_job()

        for priority in (NO_PRIORITY, HIGH_PRIORITY, LOW_PRIORITY):
            for background in (FOREGROUND_JOB, BACKGROUND_JOB):
                current_request.reset()
                current_request.priority = priority
                current_request.background = background

                self.connection_handler.send_job_request(current_request)

                queued_request = self.connection_handler.requests_awaiting_handles.popleft()
                self.assertEqual(queued_request, current_request)

                expected_cmd_type = submit_cmd_for_background_priority(background, priority)
                self.assert_sent_command(expected_cmd_type, function_name=gearman_job.func, data=gearman_job.data, unique=gearman_job.unique)

    def test_get_status_of_job(self):
        current_request = self.generate_job_request()

        self.connection_handler.send_get_status_of_job(current_request)

        self.assert_sent_command(GEARMAN_COMMAND_GET_STATUS, job_handle=current_request.get_handle())

class GearmanClientConnectionHandlerStateMachineTest(_GearmanAbstractTest):
    """Test single state transitions within a GearmanWorkerConnectionHandler"""
    client_base_class = MockGearmanClientBase
    connection_handler_class = GearmanClientConnectionHandler

    def generate_job_request(self):
        current_request = super(GearmanClientConnectionHandlerStateMachineTest, self).generate_job_request()
        job_handle = current_request.get_handle()
        self.connection_handler.handle_to_request_map[job_handle] = current_request
        return current_request

    def test_received_job_created(self):
        current_request = self.generate_job_request()

        self.connection_handler.requests_awaiting_handles.append(current_request)
        self.connection_handler.handle_to_request_map.pop(current_request.get_handle())

        current_request.state = GEARMAN_JOB_STATE_PENDING

        new_handle = str(random.random())
        self.connection_handler.recv_command(GEARMAN_COMMAND_JOB_CREATED, job_handle=new_handle)

        self.assertEqual(current_request.get_handle(), new_handle)
        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_QUEUED)
        self.assertEqual(self.connection_handler.handle_to_request_map[new_handle], current_request)

    def test_received_job_created_out_of_order(self):
        self.assertEqual(self.connection_handler.requests_awaiting_handles, collections.deque())

        # Make sure we bail cuz we have an empty queue
        self.assertRaises(InvalidClientState, self.connection_handler.recv_command, GEARMAN_COMMAND_JOB_CREATED, job_handle=None)

    def test_required_state_pending(self):
        current_request = self.generate_job_request()

        new_handle = str(random.random())

        invalid_states = [GEARMAN_JOB_STATE_QUEUED, GEARMAN_JOB_STATE_COMPLETE, GEARMAN_JOB_STATE_FAILED]
        for bad_state in invalid_states:
            current_request.state = bad_state

            # We only want to check the state of request... not die if we don't have any pending requests
            self.connection_handler.requests_awaiting_handles.append(current_request)

            self.assertRaises(InvalidClientState, self.connection_handler.recv_command, GEARMAN_COMMAND_JOB_CREATED, job_handle=new_handle)

    def test_required_state_queued(self):
        current_request = self.generate_job_request()

        job_handle = current_request.get_handle()
        new_data = str(random.random())

        invalid_states = [GEARMAN_JOB_STATE_PENDING, GEARMAN_JOB_STATE_COMPLETE, GEARMAN_JOB_STATE_FAILED]
        for bad_state in invalid_states:
            current_request.state = bad_state

            # All these commands expect to be in GEARMAN_JOB_STATE_QUEUED
            self.assertRaises(InvalidClientState, self.connection_handler.recv_command, GEARMAN_COMMAND_WORK_DATA, job_handle=job_handle, data=new_data)

            self.assertRaises(InvalidClientState, self.connection_handler.recv_command, GEARMAN_COMMAND_WORK_WARNING, job_handle=job_handle, data=new_data)

            self.assertRaises(InvalidClientState, self.connection_handler.recv_command, GEARMAN_COMMAND_WORK_STATUS, job_handle=job_handle, numerator=0.0, denominator=1.0)

            self.assertRaises(InvalidClientState, self.connection_handler.recv_command, GEARMAN_COMMAND_WORK_COMPLETE, job_handle=job_handle, data=new_data)

            self.assertRaises(InvalidClientState, self.connection_handler.recv_command, GEARMAN_COMMAND_WORK_FAIL, job_handle=job_handle)

    def test_in_flight_work_updates(self):
        current_request = self.generate_job_request()

        job_handle = current_request.get_handle()
        new_data = str(random.random())

        # Test WORK_DATA
        self.connection_handler.recv_command(GEARMAN_COMMAND_WORK_DATA, job_handle=job_handle, data=new_data)
        self.assertEqual(current_request.data_updates.popleft(), new_data)
        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_QUEUED)

        # Test WORK_WARNING
        self.connection_handler.recv_command(GEARMAN_COMMAND_WORK_WARNING, job_handle=job_handle, data=new_data)
        self.assertEqual(current_request.warning_updates.popleft(), new_data)
        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_QUEUED)

        # Test WORK_STATUS
        self.connection_handler.recv_command(GEARMAN_COMMAND_WORK_STATUS, job_handle=job_handle, numerator=0.0, denominator=1.0)

        self.assertEqual(current_request.status_updates.popleft(), (0.0, 1.0))
        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_QUEUED)

    def test_work_complete(self):
        current_request = self.generate_job_request()

        job_handle = current_request.get_handle()
        new_data = str(random.random())
        self.connection_handler.recv_command(GEARMAN_COMMAND_WORK_COMPLETE, job_handle=job_handle, data=new_data)

        self.assertEqual(current_request.result, new_data)
        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_COMPLETE)

    def test_work_fail(self):
        current_request = self.generate_job_request()

        job_handle = current_request.get_handle()
        new_data = str(random.random())
        self.connection_handler.recv_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=job_handle)

        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_FAILED)

    def test_status_request(self):
        current_request = self.generate_job_request()

        job_handle = current_request.get_handle()

        self.assertEqual(current_request.server_status, {})

        self.connection_handler.recv_command(GEARMAN_COMMAND_STATUS_RES, job_handle=job_handle, known='1', running='1', numerator='0', denominator='1')

        self.assertEqual(current_request.server_status['handle'], job_handle)
        self.assertTrue(current_request.server_status['known'])
        self.assertTrue(current_request.server_status['running'])
        self.assertEqual(current_request.server_status['numerator'], 0.0)
        self.assertEqual(current_request.server_status['denominator'], 1.0)


class GearmanWorkerConnectionHandlerInterfaceTest(_GearmanAbstractTest):
    """Test the public interface a GearmanWorker may need to call in order to update state on a GearmanWorkerConnectionHandler"""
    client_base_class = MockGearmanClientBase
    connection_handler_class = GearmanWorkerConnectionHandler

    def setUp(self):
        super(GearmanWorkerConnectionHandlerInterfaceTest, self).setUp()
        self.connection._is_connected = False
        self.connection_handler._awaiting_job_assignment = False

    def test_on_connect(self):
        expected_abilities = ['function_one', 'function_two', 'function_three']
        expected_client_id = 'my_client_id'

        self.connection._is_connected = False
        self.connection_handler.set_abilities(expected_abilities)
        self.connection_handler.set_client_id(expected_client_id)
        self.assert_no_pending_commands()

        self.connection._is_connected = True
        self.connection_handler.on_connect()

        self.assert_sent_abilities(expected_abilities)
        self.assert_sent_client_id(expected_client_id)
        self.assert_sent_command(GEARMAN_COMMAND_PRE_SLEEP)

    def test_set_abilities(self):
        expected_abilities = ['function_one', 'function_two', 'function_three']

        self.connection._is_connected = False
        self.connection_handler.set_abilities(expected_abilities)
        self.assert_no_pending_commands()

        self.connection._is_connected = True
        self.connection_handler.set_abilities(expected_abilities)
        self.assert_sent_abilities(expected_abilities)

    def test_set_client_id(self):
        expected_client_id = 'my_client_id'

        self.connection._is_connected = False
        self.connection_handler.set_client_id(expected_client_id)
        self.assert_no_pending_commands()

        self.connection._is_connected = True
        self.connection_handler.set_client_id(expected_client_id)
        self.assert_sent_client_id(expected_client_id)

    def test_send_functions(self):
        current_job = self.generate_job()

        # Test GEARMAN_COMMAND_WORK_STATUS
        self.connection_handler.send_job_status(current_job, 0, 1)
        self.assert_sent_command(GEARMAN_COMMAND_WORK_STATUS, job_handle=current_job.handle, numerator=0, denominator=1)

        # Test GEARMAN_COMMAND_WORK_COMPLETE
        self.connection_handler.send_job_complete(current_job, 'completion data')
        self.assert_sent_command(GEARMAN_COMMAND_WORK_COMPLETE, job_handle=current_job.handle, data='completion data')

        # Test GEARMAN_COMMAND_WORK_FAIL
        self.connection_handler.send_job_failure(current_job)
        self.assert_sent_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=current_job.handle)

        # Test GEARMAN_COMMAND_WORK_EXCEPTION
        self.connection_handler.send_job_exception(current_job, 'exception data')
        self.assert_sent_command(GEARMAN_COMMAND_WORK_EXCEPTION, job_handle=current_job.handle, data='exception data')

        # Test GEARMAN_COMMAND_WORK_DATA
        self.connection_handler.send_job_data(current_job, 'job data')
        self.assert_sent_command(GEARMAN_COMMAND_WORK_DATA, job_handle=current_job.handle, data='job data')

        # Test GEARMAN_COMMAND_WORK_WARNING
        self.connection_handler.send_job_warning(current_job, 'job warning')
        self.assert_sent_command(GEARMAN_COMMAND_WORK_WARNING, job_handle=current_job.handle, data='job warning')

    def assert_sent_abilities(self, expected_abilities):
        self.assert_sent_command(GEARMAN_COMMAND_RESET_ABILITIES)
        for ability in expected_abilities:
            self.assert_sent_command(GEARMAN_COMMAND_CAN_DO, function_name=ability)

    def assert_sent_client_id(self, expected_client_id):
        self.assert_sent_command(GEARMAN_COMMAND_SET_CLIENT_ID, client_id=expected_client_id)


class GearmanWorkerConnectionHandlerStateMachineTest(_GearmanAbstractTest):
    """Test multiple state transitions within a GearmanWorkerConnectionHandler

    End to end tests without a server
    """
    client_base_class = MockGearmanClientBase
    connection_handler_class = GearmanWorkerConnectionHandler

    def setUp(self):
        super(GearmanWorkerConnectionHandlerStateMachineTest, self).setUp()
        self.connection_handler._connection_abilities = ['test_function_name']

    def test_wakeup_work(self):
        self.move_to_state_wakeup()

        self.move_to_state_job_assign_uniq(self.generate_job_dict())

        self.move_to_state_no_job()

    def test_wakeup_sleep_wakup_work(self):
        self.move_to_state_wakeup()

        self.move_to_state_no_job()

        self.move_to_state_wakeup()

        self.move_to_state_job_assign_uniq(self.generate_job_dict())

        self.move_to_state_no_job()

    def test_multiple_wakeup_then_no_work(self):
        # Awaken the state machine... then give it no work
        self.move_to_state_wakeup()

        for _ in range(5):
            self.connection_handler.recv_command(GEARMAN_COMMAND_NOOP)

        self.assertTrue(self.connection_handler._awaiting_job_assignment)

        # Pretend like the server has no work... do nothing
        # Moving to state NO_JOB will make sure there's only 1 item on the queue
        self.move_to_state_no_job()

    def test_multiple_work(self):
        self.move_to_state_wakeup()

        self.move_to_state_job_assign_uniq(self.generate_job_dict())

        self.move_to_state_job_assign_uniq(self.generate_job_dict())

        self.move_to_state_job_assign_uniq(self.generate_job_dict())

        # After this job completes, we're going to greedily ask for more jobs
        self.move_to_state_no_job()

    def move_to_state_wakeup(self):
        self.assert_no_pending_commands()
        self.assertFalse(self.connection_handler._awaiting_job_assignment)

        self.connection_handler.recv_command(GEARMAN_COMMAND_NOOP)

    def move_to_state_no_job(self):
        """Move us to the NO_JOB state...
        
        1) We should've most recently sent only a single GEARMAN_COMMAND_GRAB_JOB_UNIQ
        2) We should be awaiting job assignment
        3) Once we receive a NO_JOB, we should say we're going back to sleep"""
        self.assert_awaiting_job()

        self.connection_handler.recv_command(GEARMAN_COMMAND_NO_JOB)

        # We should be asleep... which means no pending jobs and we're not awaiting job assignment
        self.assert_sent_command(GEARMAN_COMMAND_PRE_SLEEP)
        self.assert_no_pending_commands()
        self.assertFalse(self.connection_handler._awaiting_job_assignment)

    def move_to_state_job_assign_uniq(self, fake_job):
        """Move us to the JOB_ASSIGN_UNIQ state...
        
        1) We should've most recently sent only a single GEARMAN_COMMAND_GRAB_JOB_UNIQ
        2) We should be awaiting job assignment
        3) The job we receive should be the one we expected"""
        self.assert_awaiting_job()

        ### NOTE: This recv_command does NOT send out a GEARMAN_COMMAND_JOB_COMPLETE or GEARMAN_COMMAND_JOB_FAIL
        ###           as we're using a MockGearmanClientBase with a method that only queues the job
        self.connection_handler.recv_command(GEARMAN_COMMAND_JOB_ASSIGN_UNIQ, **fake_job)

        current_job = self.client_base.worker_job_queues[self.connection_handler].popleft()
        self.assertEqual(current_job.handle, fake_job['job_handle'])
        self.assertEqual(current_job.func, fake_job['function_name'])
        self.assertEqual(current_job.unique, fake_job['unique'])
        self.assertEqual(current_job.data, fake_job['data'])

        # At the end of recv_command(GEARMAN_COMMAND_JOB_ASSIGN_UNIQ)
        # We should have greedily requested a job
        self.assertTrue(self.connection_handler._awaiting_job_assignment)

    def assert_awaiting_job(self):
        self.assert_sent_command(GEARMAN_COMMAND_GRAB_JOB_UNIQ)
        self.assert_no_pending_commands()
        self.assertTrue(self.connection_handler._awaiting_job_assignment)

class GearmanManagerTest(unittest.TestCase):
    def setUp(self):
        self.manager = GearmanManager(host_list=job_servers, blocking_timeout=1.0)

    def testStatus(self):
        status = self.manager.status()
        self.failUnless(type(status) is tuple)

    def testVersion(self):
        version = self.manager.version()
        self.failUnless('.' in version)

    def testWorkers(self):
        workers = self.manager.workers()
        self.failUnless(type(workers) is tuple)

if __name__ == '__main__':
    unittest.main()