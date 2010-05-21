import collections
import random
import unittest

import sys
sys.path.insert(0, '/nail/home/mtai/pg/python-gearman')

from gearman.client import GearmanClient
from gearman.client_handler import GearmanClientCommandHandler

from gearman.constants import BACKGROUND_JOB, FOREGROUND_JOB, NO_PRIORITY, HIGH_PRIORITY, LOW_PRIORITY
from gearman.errors import ServerUnavailable, InvalidClientState
from gearman.job import GEARMAN_JOB_STATE_PENDING, GEARMAN_JOB_STATE_QUEUED, GEARMAN_JOB_STATE_FAILED, GEARMAN_JOB_STATE_COMPLETE
from gearman.protocol import submit_cmd_for_background_priority, GEARMAN_COMMAND_STATUS_RES, GEARMAN_COMMAND_GET_STATUS, GEARMAN_COMMAND_JOB_CREATED, \
    GEARMAN_COMMAND_WORK_STATUS, GEARMAN_COMMAND_WORK_FAIL, GEARMAN_COMMAND_WORK_COMPLETE, GEARMAN_COMMAND_WORK_DATA, GEARMAN_COMMAND_WORK_WARNING

from tests._core_testing import _GearmanAbstractTest, MockGearmanConnectionManager, MockGearmanConnection

class MockGearmanClient(GearmanClient, MockGearmanConnectionManager):
    pass

class ClientTest(_GearmanAbstractTest):
    """Test the public client interface"""
    connection_manager_class = MockGearmanClient
    command_handler_class = GearmanClientCommandHandler

    def setUp(self):
        super(ClientTest, self).setUp()
        self.original_poll_connections_once = self.connection_manager.poll_connections_once

    def tearDown(self):
        super(ClientTest, self).tearDown()
        self.connection_manager.poll_connections_once = self.original_poll_connections_once

    def generate_job_request(self):
        current_request = super(ClientTest, self).generate_job_request()

        job_handle = current_request.get_handle()
        self.command_handler.handle_to_request_map[job_handle] = current_request
        return current_request

    def test_connection_rotation_for_requests(self):
        # Spin up a bunch of imaginary gearman connections
        failed_connection = MockGearmanConnection()
        failed_connection._should_fail_on_bind = True
        failed_connection._is_connected = False

        failed_then_retried_connection = MockGearmanConnection()
        failed_then_retried_connection._should_fail_on_connect = True
        failed_then_retried_connection._is_connected = True

        good_connection = MockGearmanConnection()
        good_connection._should_fail_on_bind = False
        good_connection._is_connected = True

        self.connection_manager.connection_list = [failed_connection, failed_then_retried_connection, good_connection]

        # Register all our connections
        current_request = self.generate_job_request()

        self.failIf(current_request in self.connection_manager.request_to_rotating_connection_queue)

        # Make sure that when we start up, we get our good connection
        chosen_conn = self.connection_manager.choose_request_connection(current_request)
        self.assertEqual(chosen_conn, good_connection)

        # No state changed so we should still go there
        chosen_conn = self.connection_manager.choose_request_connection(current_request)
        self.assertEqual(chosen_conn, good_connection)

        # Pretend like our good connection died so we'll need to choose somethign else
        good_connection._should_fail_on_bind = True
        good_connection._is_connected = False

        failed_then_retried_connection._should_fail_on_connect = False
        failed_then_retried_connection._is_connected = True

        # Make sure we rotate good_connection and failed_connection out
        chosen_conn = self.connection_manager.choose_request_connection(current_request)
        self.assertEqual(chosen_conn, failed_then_retried_connection)

    def test_no_connections_for_rotation_for_requests(self):
        self.connection_manager.connection_list = []
        self.connection_manager.command_handlers = {}

        current_request = self.generate_job_request()

        # No connections == death
        self.assertRaises(ServerUnavailable, self.connection_manager.choose_request_connection, current_request)

        # Spin up a bunch of imaginary gearman connections
        failed_connection = MockGearmanConnection()
        failed_connection._should_fail_on_bind = True
        failed_connection._is_connected = False
        self.connection_manager.connection_list.append(failed_connection)

        # All failed connections == death
        self.assertRaises(ServerUnavailable, self.connection_manager.choose_request_connection, current_request)

    def test_multiple_fg_job_submission(self):
        submitted_job_count = 5
        expected_job_list = [self.generate_job() for _ in xrange(submitted_job_count)]
        def mark_jobs_created(connections, timeout=None):
            for current_job in expected_job_list:
                self.command_handler.recv_command(GEARMAN_COMMAND_JOB_CREATED, job_handle=current_job.handle)

        self.connection_manager.poll_connections_once = mark_jobs_created

        job_dictionaries = [current_job.to_dict() for current_job in expected_job_list]

        # Test multiple job submission
        job_requests = self.connection_manager.submit_multiple_jobs(job_dictionaries, background=FOREGROUND_JOB)
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
            self.command_handler.recv_command(GEARMAN_COMMAND_JOB_CREATED, job_handle=expected_job.handle)

        self.connection_manager.poll_connections_once = mark_job_created
        job_request = self.connection_manager.submit_job(expected_job.func, expected_job.data, unique=expected_job.unique, background=BACKGROUND_JOB, priority=LOW_PRIORITY)

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

        self.connection_manager.poll_connections_once = job_failed_submission
        job_request = self.connection_manager.submit_job(expected_job.func, expected_job.data, unique=expected_job.unique, priority=HIGH_PRIORITY, timeout=0.01)

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
                self.command_handler.recv_command(GEARMAN_COMMAND_WORK_COMPLETE, job_handle=completed_request.get_handle(), data='12345')
                self.command_handler.recv_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=failed_request.get_handle())
                self.update_requests = False

        self.connection_manager.poll_connections_once = multiple_job_updates

        finished_requests = self.connection_manager.wait_until_jobs_completed([completed_request, failed_request, timeout_request], timeout=0.01)
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

    def test_get_job_status(self):
        self.connection.connect()

        single_request = self.generate_job_request()
        single_request.state = GEARMAN_JOB_STATE_QUEUED

        def retrieve_status(connections, timeout=None):
            self.command_handler.recv_command(GEARMAN_COMMAND_STATUS_RES, job_handle=single_request.get_handle(), known='1', running='0', numerator='0.0', denominator='1.0')

        self.connection_manager.poll_connections_once = retrieve_status

        job_request = self.connection_manager.get_job_status(single_request)
        request_status = job_request.server_status
        self.failUnless(request_status)
        self.assertTrue(request_status['known'])
        self.assertFalse(request_status['running'])
        self.assertEqual(request_status['numerator'], 0.0)
        self.assertEqual(request_status['denominator'], 1.0)
        self.assertFalse(job_request.timed_out)

    def test_get_job_status_timeout(self):
        self.connection.connect()

        single_request = self.generate_job_request()
        single_request.state = GEARMAN_JOB_STATE_QUEUED

        def retrieve_status_timeout(connections, timeout=None):
            pass

        self.connection_manager.poll_connections_once = retrieve_status_timeout

        job_request = self.connection_manager.get_job_status(single_request, timeout=0.01)
        self.assertTrue(job_request.timed_out)


class ClientCommandHandlerInterfaceTest(_GearmanAbstractTest):
    """Test the public interface a GearmanClient may need to call in order to update state on a GearmanClientCommandHandler"""
    connection_manager_class = MockGearmanClient
    command_handler_class = GearmanClientCommandHandler

    def test_send_job_request(self):
        current_request = self.generate_job_request()
        gearman_job = current_request.get_job()

        for priority in (NO_PRIORITY, HIGH_PRIORITY, LOW_PRIORITY):
            for background in (FOREGROUND_JOB, BACKGROUND_JOB):
                current_request.reset()
                current_request.priority = priority
                current_request.background = background

                self.command_handler.send_job_request(current_request)

                queued_request = self.command_handler.requests_awaiting_handles.popleft()
                self.assertEqual(queued_request, current_request)

                expected_cmd_type = submit_cmd_for_background_priority(background, priority)
                self.assert_sent_command(expected_cmd_type, function_name=gearman_job.func, data=gearman_job.data, unique=gearman_job.unique)

    def test_get_status_of_job(self):
        current_request = self.generate_job_request()

        self.command_handler.send_get_status_of_job(current_request)

        self.assert_sent_command(GEARMAN_COMMAND_GET_STATUS, job_handle=current_request.get_handle())

class ClientCommandHandlerStateMachineTest(_GearmanAbstractTest):
    """Test single state transitions within a GearmanWorkerCommandHandler"""
    connection_manager_class = MockGearmanClient
    command_handler_class = GearmanClientCommandHandler

    def generate_job_request(self):
        current_request = super(ClientCommandHandlerStateMachineTest, self).generate_job_request()
        job_handle = current_request.get_handle()
        self.command_handler.handle_to_request_map[job_handle] = current_request
        return current_request

    def test_received_job_created(self):
        current_request = self.generate_job_request()

        self.command_handler.requests_awaiting_handles.append(current_request)
        self.command_handler.handle_to_request_map.pop(current_request.get_handle())

        current_request.state = GEARMAN_JOB_STATE_PENDING

        new_handle = str(random.random())
        self.command_handler.recv_command(GEARMAN_COMMAND_JOB_CREATED, job_handle=new_handle)

        self.assertEqual(current_request.get_handle(), new_handle)
        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_QUEUED)
        self.assertEqual(self.command_handler.handle_to_request_map[new_handle], current_request)

    def test_received_job_created_out_of_order(self):
        self.assertEqual(self.command_handler.requests_awaiting_handles, collections.deque())

        # Make sure we bail cuz we have an empty queue
        self.assertRaises(InvalidClientState, self.command_handler.recv_command, GEARMAN_COMMAND_JOB_CREATED, job_handle=None)

    def test_required_state_pending(self):
        current_request = self.generate_job_request()

        new_handle = str(random.random())

        invalid_states = [GEARMAN_JOB_STATE_QUEUED, GEARMAN_JOB_STATE_COMPLETE, GEARMAN_JOB_STATE_FAILED]
        for bad_state in invalid_states:
            current_request.state = bad_state

            # We only want to check the state of request... not die if we don't have any pending requests
            self.command_handler.requests_awaiting_handles.append(current_request)

            self.assertRaises(InvalidClientState, self.command_handler.recv_command, GEARMAN_COMMAND_JOB_CREATED, job_handle=new_handle)

    def test_required_state_queued(self):
        current_request = self.generate_job_request()

        job_handle = current_request.get_handle()
        new_data = str(random.random())

        invalid_states = [GEARMAN_JOB_STATE_PENDING, GEARMAN_JOB_STATE_COMPLETE, GEARMAN_JOB_STATE_FAILED]
        for bad_state in invalid_states:
            current_request.state = bad_state

            # All these commands expect to be in GEARMAN_JOB_STATE_QUEUED
            self.assertRaises(InvalidClientState, self.command_handler.recv_command, GEARMAN_COMMAND_WORK_DATA, job_handle=job_handle, data=new_data)

            self.assertRaises(InvalidClientState, self.command_handler.recv_command, GEARMAN_COMMAND_WORK_WARNING, job_handle=job_handle, data=new_data)

            self.assertRaises(InvalidClientState, self.command_handler.recv_command, GEARMAN_COMMAND_WORK_STATUS, job_handle=job_handle, numerator=0.0, denominator=1.0)

            self.assertRaises(InvalidClientState, self.command_handler.recv_command, GEARMAN_COMMAND_WORK_COMPLETE, job_handle=job_handle, data=new_data)

            self.assertRaises(InvalidClientState, self.command_handler.recv_command, GEARMAN_COMMAND_WORK_FAIL, job_handle=job_handle)

    def test_in_flight_work_updates(self):
        current_request = self.generate_job_request()

        job_handle = current_request.get_handle()
        new_data = str(random.random())

        # Test WORK_DATA
        self.command_handler.recv_command(GEARMAN_COMMAND_WORK_DATA, job_handle=job_handle, data=new_data)
        self.assertEqual(current_request.data_updates.popleft(), new_data)
        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_QUEUED)

        # Test WORK_WARNING
        self.command_handler.recv_command(GEARMAN_COMMAND_WORK_WARNING, job_handle=job_handle, data=new_data)
        self.assertEqual(current_request.warning_updates.popleft(), new_data)
        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_QUEUED)

        # Test WORK_STATUS
        self.command_handler.recv_command(GEARMAN_COMMAND_WORK_STATUS, job_handle=job_handle, numerator=0.0, denominator=1.0)

        self.assertEqual(current_request.status_updates.popleft(), (0.0, 1.0))
        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_QUEUED)

    def test_work_complete(self):
        current_request = self.generate_job_request()

        job_handle = current_request.get_handle()
        new_data = str(random.random())
        self.command_handler.recv_command(GEARMAN_COMMAND_WORK_COMPLETE, job_handle=job_handle, data=new_data)

        self.assertEqual(current_request.result, new_data)
        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_COMPLETE)

    def test_work_fail(self):
        current_request = self.generate_job_request()

        job_handle = current_request.get_handle()
        new_data = str(random.random())
        self.command_handler.recv_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=job_handle)

        self.assertEqual(current_request.state, GEARMAN_JOB_STATE_FAILED)

    def test_status_request(self):
        current_request = self.generate_job_request()

        job_handle = current_request.get_handle()

        self.assertEqual(current_request.server_status, {})

        self.command_handler.recv_command(GEARMAN_COMMAND_STATUS_RES, job_handle=job_handle, known='1', running='1', numerator='0', denominator='1')

        self.assertEqual(current_request.server_status['handle'], job_handle)
        self.assertTrue(current_request.server_status['known'])
        self.assertTrue(current_request.server_status['running'])
        self.assertEqual(current_request.server_status['numerator'], 0.0)
        self.assertEqual(current_request.server_status['denominator'], 1.0)

if __name__ == '__main__':
    unittest.main()
