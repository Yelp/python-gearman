import atexit
import logging
import os, sys, signal, threading
import unittest, time, socket
import collections
import random

from gearman import GearmanClient, GearmanWorker
from gearman.connection import GearmanConnection
from gearman.errors import ConnectionError
from gearman.manager import GearmanManager
from gearman.server import GearmanServer
from gearman.task import Task
from gearman.protocol import *
from gearman.job import GearmanJob, GEARMAN_JOB_STATE_PENDING, GEARMAN_JOB_STATE_QUEUED, GEARMAN_JOB_STATE_FAILED, GEARMAN_JOB_STATE_COMPLETE, GEARMAN_JOB_STATE_TIMEOUT
from gearman.constants import BACKGROUND_JOB
from gearman._client_base import GearmanConnectionHandler, GearmanClientBase
from gearman.worker import GearmanWorkerConnectionHandler, GearmanWorker
job_servers = ["127.0.0.1"]

class FailedError(Exception):
    pass

def echo_fxn(gearman_worker, job):
    return job.data

def fail_fxn(gearman_worker, job):
    raise FailedError()

def sleep_fxn(gearman_worker, job):
    time.sleep(float(job.data))
    return job.data

def work_data_fxn(gearman_worker, job):
    max_count = int(job.data)
    for count in xrange(max_count):
        gearman_worker.send_job_data(job, count)

    return max_count

def work_status_fxn(gearman_worker, job):
    max_status = int(job.data)
    for current_status in xrange(max_status):
        gearman_worker.send_job_status(job, float(current_status), float(max_status))

    return float(max_status)

def work_warning_fxn(gearman_worker, job):
    gearman_worker.send_job_warning(job, "THIS")
    gearman_worker.send_job_warning(job, "IS")
    gearman_worker.send_job_warning(job, "A")
    gearman_worker.send_job_warning(job, "TEST")
    return ''

class MockGearmanConnection(GearmanConnection):
    def __init__(self, *largs, **kwargs):
        super(MockGearmanConnection, self).__init__(*largs, **kwargs)
        self._should_be_connected = False

    def is_connected(self):
        return self._should_be_connected

class MockGearmanClientBase(GearmanClientBase):
    """Handy mock client base to test Worker/Client/Abstract ConnectionHandlers"""
    def __init__(self):
        super(MockGearmanClientBase, self).__init__()
        # ConnectionHandler to command queue map
        self.command_queues = collections.defaultdict(collections.deque)

        # ConnectionHandler to job queue maps
        self.worker_job_queues = collections.defaultdict(collections.deque)

    # Catch all IO here in the test_command_queue
    def send_command(self, gearman_connection, cmd_type, cmd_args):
        actual_connection_handler = self.connection_handlers[gearman_connection]
        self.command_queues[actual_connection_handler].append((cmd_type, cmd_args))

    def on_job_execute(self, connection_handler, current_job):
        self.worker_job_queues[connection_handler].append(current_job)

    def handle_error(self, gearman_connection):
        pass

class MockGearmanConnectionHandler(GearmanConnectionHandler):
    def __init__(self, *largs, **kwargs):
        super(MockGearmanConnectionHandler, self).__init__(*largs, **kwargs)
        self.sent_command_queue = collections.deque()
        self.recv_command_queue = collections.deque()

    def send_command(self, cmd_type, **cmd_args):
        self.sent_command_queue.append((cmd_type, cmd_args))
        sent_result = super(MockGearmanConnectionHandler, self).send_command(cmd_type, **cmd_args)
        return sent_result

    def recv_command(self, cmd_type, cmd_args):
        self.recv_command_queue.append((cmd_type, cmd_args)) 
        recv_result = super(MockGearmanConnectionHandler, self).recv_command(cmd_type, cmd_args)
        return recv_result

    def recv_noop(self):
        pass

class GearmanAbstractTestCase(unittest.TestCase):
    connection_handler_class = MockGearmanConnectionHandler
    
    def setUp(self):
        self.connection = MockGearmanConnection(hostname=None)

        self.client_base = MockGearmanClientBase()
        self.connection_handler = self.connection_handler_class(self.client_base, self.connection)

        self.client_base.connection_handlers[self.connection] = self.connection_handler

    def assert_sent_command(self, expected_cmd_type, **expected_cmd_args):
        # Make sure any commands we're passing through the ConnectionHandler gets properly passed through to the client base
        client_cmd_type, client_cmd_args = self.client_base.command_queues[self.connection_handler].popleft()
        self.assertEqual(client_cmd_type, expected_cmd_type)
        self.assertEqual(client_cmd_args, expected_cmd_args)

    def assert_no_command_sent(self):
        self.failIf(self.client_base.command_queues[self.connection_handler])

class GearmanConnectionHandlerTest(GearmanAbstractTestCase):
    def test_recv_command(self):
        self.connection_handler.recv_command(GEARMAN_COMMAND_NOOP, {})
        self.assert_recv_command(GEARMAN_COMMAND_NOOP, {})

        # The mock handler never implemented "recv_all_yours" so we should get an attribute error here
        self.assertRaises(ValueError, self.connection_handler.recv_command, GEARMAN_COMMAND_ALL_YOURS, {})

    def test_send_command(self):
        self.connection_handler.send_command(GEARMAN_COMMAND_NOOP)
        self.assert_sent_command(GEARMAN_COMMAND_NOOP)
 
        # The mock handler never implemented "recv_all_yours" so we should get an attribute error here
        self.connection_handler.send_command(GEARMAN_COMMAND_ECHO_REQ, text="hello world")
        self.assert_sent_command(GEARMAN_COMMAND_ECHO_REQ, text="hello world")

    def assert_recv_command(self, expected_cmd_type, expected_cmd_args):
        cmd_type, cmd_args = self.connection_handler.recv_command_queue.popleft()
        self.assertEqual(cmd_type, expected_cmd_type)
        self.assertEqual(cmd_args, expected_cmd_args)

    def assert_sent_command(self, expected_cmd_type, **expected_cmd_args):
        # All commands should be sent via the ConnectionHandler
        handler_cmd_type, handler_cmd_args = self.connection_handler.sent_command_queue.popleft()
        self.assertEqual(handler_cmd_type, expected_cmd_type)
        self.assertEqual(handler_cmd_args, expected_cmd_args)

        super(GearmanConnectionHandlerTest, self).assert_sent_command(expected_cmd_type, **expected_cmd_args)

class GearmanWorkerConnectionHandlerTest(GearmanAbstractTestCase):
    """Exhaustively test our WorkerConnectionHandler..."""
    connection_handler_class = GearmanWorkerConnectionHandler

    def setUp(self):
        super(GearmanWorkerConnectionHandlerTest, self).setUp()
        self.connection._should_be_connected = False
        self.connection_handler._awaiting_job_assignment = False

    def generate_job(self):
        EXPECTED_JOB_HANDLE = str(random.random())
        EXPECTED_FUNCTION_NAME = 'fake_function_name'
        EXPECTED_UNIQUE = random.random()
        EXPECTED_DATA = random.random()

        return GearmanJob(self.connection, EXPECTED_JOB_HANDLE, EXPECTED_FUNCTION_NAME, EXPECTED_UNIQUE, EXPECTED_DATA)

    def test_request_job(self):
        # Test the noop case for request_job... if we know we're already awaiting job assignment, no need to ask again
        self.connection_handler._awaiting_job_assignment = True
        
        self.connection_handler.request_job()
        
        self.assert_no_command_sent()
        self.assertTrue(self.connection_handler._awaiting_job_assignment)

        # Test the active case for request_job... if we know we are NOT awaiting job assignment, we need to ask for work
        self.connection_handler._awaiting_job_assignment = False
        
        self.connection_handler.request_job()

        self.assert_sent_command(GEARMAN_COMMAND_GRAB_JOB_UNIQ)
        self.assertTrue(self.connection_handler._awaiting_job_assignment)

    def test_set_abilities(self):
        expected_abilities = ['function_one', 'function_two', 'function_three']

        self.connection._should_be_connected = False
        self.connection_handler.set_abilities(expected_abilities)
        self.assert_no_command_sent()

        self.connection._should_be_connected = True
        self.connection_handler.set_abilities(expected_abilities)
        self.assert_sent_command(GEARMAN_COMMAND_RESET_ABILITIES)
        for ability in expected_abilities:
            self.assert_sent_command(GEARMAN_COMMAND_CAN_DO, function_name=ability)

    def test_set_client_id(self):
        expected_client_id = "my_client_id"

        self.connection._should_be_connected = False
        self.connection_handler.set_client_id(expected_client_id)
        self.assert_no_command_sent()

        self.connection._should_be_connected = True
        self.connection_handler.set_client_id(expected_client_id)
        self.assert_sent_command(GEARMAN_COMMAND_SET_CLIENT_ID, data=expected_client_id)

    def test_on_job_completion(self):
        current_job = self.generate_job()
        job_result = str(random.random())
        self.connection_handler.on_job_complete(current_job, job_result)
        self.assert_sent_command(GEARMAN_COMMAND_WORK_COMPLETE, job_handle=current_job.handle, data=job_result)

    def test_on_job_exception(self):
        current_job = self.generate_job()
        job_exception = None
        self.connection_handler.on_job_exception(current_job, None)
        self.assert_sent_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=current_job.handle)

    def test_send_functions(self):
        current_job = self.generate_job()
        
        # Test GEARMAN_COMMAND_WORK_STATUS
        self.connection_handler.send_job_status(current_job, 0, 1)
        self.assert_sent_command(GEARMAN_COMMAND_WORK_STATUS, job_handle=current_job.handle, numerator=0, denominator=1)
        
        # Test GEARMAN_COMMAND_WORK_COMPLETE
        self.connection_handler.send_job_complete(current_job, "completion data")
        self.assert_sent_command(GEARMAN_COMMAND_WORK_COMPLETE, job_handle=current_job.handle, data="completion data")
        
        # Test GEARMAN_COMMAND_WORK_FAIL
        self.connection_handler.send_job_failure(current_job)
        self.assert_sent_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=current_job.handle)

        # Test GEARMAN_COMMAND_WORK_EXCEPTION
        self.connection_handler.send_job_exception(current_job, "exception data")
        self.assert_sent_command(GEARMAN_COMMAND_WORK_EXCEPTION, job_handle=current_job.handle, data="exception data")
        self.assert_sent_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=current_job.handle)
        
        # Test GEARMAN_COMMAND_WORK_DATA
        self.connection_handler.send_job_data(current_job, "job data")
        self.assert_sent_command(GEARMAN_COMMAND_WORK_DATA, job_handle=current_job.handle, data="job data")

        # Test GEARMAN_COMMAND_WORK_WARNING
        self.connection_handler.send_job_warning(current_job, "job warning")
        self.assert_sent_command(GEARMAN_COMMAND_WORK_WARNING, job_handle=current_job.handle, data="job warning")

    def test_recv_noop(self):
        self.connection_handler.recv_noop()
        self.assert_sent_command(GEARMAN_COMMAND_GRAB_JOB_UNIQ)

        self.assertTrue(self.connection_handler._awaiting_job_assignment)

    def test_recv_no_job(self):
        self.connection_handler._awaiting_job_assignment = True

        self.connection_handler.recv_no_job()

        self.assert_sent_command(GEARMAN_COMMAND_PRE_SLEEP)
        self.assertFalse(self.connection_handler._awaiting_job_assignment)

    def test_recv_job_assign_uniq(self):
        current_job = self.generate_job()
        job_assign_parameters = dict(job_handle=current_job.handle, function_name=current_job.func, unique=current_job.unique, data=current_job.data)

        # Make sure that a ConnectionHandler unaware of any functions throws an AssertionError
        self.assertRaises(AssertionError, self.connection_handler.recv_job_assign_uniq, **job_assign_parameters)

        # Make our ConnectionHandler is aware of our function so it doesn't throw an exception
        self.connection_handler._connection_abilities = set([current_job.func])

        # WorkerConnectionHandler will STILL bail as we weren't expecting a job
        self.assertRaises(AssertionError, self.connection_handler.recv_job_assign_uniq, **job_assign_parameters)

        # WorkerConnectionHandler should pass with flying colors now
        self.connection_handler._awaiting_job_assignment = True
        self.connection_handler.recv_job_assign_uniq(**job_assign_parameters)

        # Let's make sure that our mock worker has received the work we were expecting
        work_queue = self.client_base.worker_job_queues[self.connection_handler]
        received_job = work_queue.popleft()
        self.assertEqual(received_job.handle, current_job.handle)
        self.assertEqual(received_job.func, current_job.func)
        self.assertEqual(received_job.unique, current_job.unique)
        self.assertEqual(received_job.data, current_job.data)

        # After all our work is complete, we want to make sure we were greedy
        self.assert_sent_command(GEARMAN_COMMAND_GRAB_JOB_UNIQ)

class TestConnection(GearmanAbstractTestCase):
    def setUp(self):
        self.connection = GearmanConnection(job_servers[0], blocking_timeout=2.0)
        self.connection.connect()

    def test_no_args(self):
        self.connection.send_command(GEARMAN_COMMAND_ECHO_REQ, dict(text=""))
        cmd_tuple = self.connection.recv_command()
        self.failUnless(cmd_tuple)

        cmd_type, cmd_args = cmd_tuple
        self.failUnlessEqual(cmd_type, GEARMAN_COMMAND_ECHO_RES)

    def test_with_args(self):
        self.connection.send_command(GEARMAN_COMMAND_SUBMIT_JOB, dict(function_name="echo", unique="%s" % time.time(), data="tea"))
        cmd_tuple = self.connection.recv_command()
        self.failUnless(cmd_tuple)

        cmd_type, cmd_args = cmd_tuple
        self.failUnlessEqual(cmd_type, GEARMAN_COMMAND_JOB_CREATED)

class TestGearman(GearmanAbstractTestCase):
    def setUp(self):
        self.worker = GearmanWorker(job_servers)
        self.worker.register_function("echo", echo_fxn)
        self.worker.register_function("fail", fail_fxn)
        self.worker.register_function("sleep", sleep_fxn)
        self.worker.register_function("work_data", work_data_fxn)
        self.worker.register_function("work_status", work_status_fxn)
        self.worker.register_function("work_warning", work_warning_fxn)

        import thread
        self.worker_thread = thread.start_new_thread(self.worker.work, tuple()) # TODO: Shouldn't use threads.. but we do for now (also, the thread is never terminated)
        self.client = GearmanClient(job_servers)

    def _submit_job_and_wait(self, function_name, data, unique=None, priority=NO_PRIORITY, background=FOREGROUND_JOB, timeout=None):
        job_request = self.client.submit_job(function_name, data, unique=unique, priority=priority, background=background)
        completed_jobs = self.client.wait_for_job_completion([job_request], timeout=timeout)
        return completed_jobs[0]

    def tearDown(self):
        del self.worker
        del self.client

    def test_job_complete(self):
        job_request = self._submit_job_and_wait("echo", "bar")
        self.failUnlessEqual(job_request.result, 'bar')

    def test_job_failed(self):
        job_request = self._submit_job_and_wait("fail", "bar")
        self.failUnlessEqual(job_request.state, GEARMAN_JOB_STATE_FAILED)

    def test_job_timeout(self):
        job_request = self._submit_job_and_wait("sleep", "0.1", timeout=1.0)
        self.failUnlessEqual(job_request.state, GEARMAN_JOB_STATE_COMPLETE)
        self.failUnlessEqual(job_request.result, "0.1")
    
        job_request = self._submit_job_and_wait("sleep", "1.5", timeout=1.0)
        self.failUnlessEqual(job_request.state, GEARMAN_JOB_STATE_TIMEOUT)
        self.failUnlessEqual(job_request.result, None)

    def test_job_failure_then_complete(self):
        job_request = self._submit_job_and_wait("fail", "bar")
        self.failUnlessEqual(job_request.state, GEARMAN_JOB_STATE_FAILED)

        job_request = self._submit_job_and_wait("echo", "bar")
        self.failUnlessEqual(job_request.result, 'bar')

    def test_job_data(self):
        job_request = self._submit_job_and_wait("work_data", "5")
        self.failUnlessEqual(len(job_request.data_updates), 5)
        for count in xrange(5):
            self.failUnlessEqual(job_request.data_updates.popleft(), str(count))

        self.failUnlessEqual(job_request.result, "5")
    
    def test_job_status(self):
        job_request = self._submit_job_and_wait("work_status", "10")
        self.failUnlessEqual(len(job_request.status_updates), 10)
        for count in xrange(10):
            self.failUnlessEqual(job_request.status_updates.popleft(), (float(count), 10.0))

        self.failUnlessEqual(job_request.result, str(10.0))
    
    def test_job_warning(self):
        job_request = self._submit_job_and_wait("work_warning", "")

        self.failUnlessEqual(len(job_request.warning_updates), 4)
        self.failUnlessEqual(job_request.warning_updates.popleft(), "THIS")
        self.failUnlessEqual(job_request.warning_updates.popleft(), "IS")
        self.failUnlessEqual(job_request.warning_updates.popleft(), "A")
        self.failUnlessEqual(job_request.warning_updates.popleft(), "TEST")
        self.failUnlessEqual(job_request.result, '')

    def test_multiple_jobs(self):
        echo_strings = ['foo', 'bar', 'hi', 'there']
        jobs_to_submit = []
        for echo_str in echo_strings:
            jobs_to_submit.append(dict(function_name="echo", data=echo_str, unique=echo_str))

        list_of_job_requests = self.client.submit_job_list(jobs_to_submit)
        self.client.wait_for_job_completion(list_of_job_requests)

        for job_request in list_of_job_requests:
            self.failUnlessEqual(job_request.gearman_job.data, job_request.result)
            self.failUnlessEqual(job_request.state, GEARMAN_JOB_STATE_COMPLETE)

    def test_status_req(self):
        job_request = self._submit_job_and_wait("sleep", "0.5", background=BACKGROUND_JOB)
        self.failUnlessEqual(job_request.state, GEARMAN_JOB_STATE_QUEUED)
        self.failUnlessEqual(job_request.result, None)

        status_response = self.client.get_status(job_request, timeout=1.0)
        self.failUnless(status_response['known'])
        self.failUnless(status_response['running'])

        time.sleep(1.0)

        status_response = self.client.get_status(job_request, timeout=1.0)
        self.failIf(status_response['known'])
        self.failIf(status_response['running'])


class TestManager(GearmanAbstractTestCase):
    def setUp(self):
        self.manager = GearmanManager(job_servers[0])

    def testStatus(self):
        status = self.manager.status()
        self.failUnless(isinstance(status, dict))

    def testVersion(self):
        version = self.manager.version()
        self.failUnless('.' in version)

    def testWorkers(self):
        workers = self.manager.workers()
        self.failUnless(isinstance(workers, list))

SERVER_PID = None

def start_server():
    # This is super gross.
    # We need to spin up a local copy of GearmanServer so we can test our clients and workers
    # Here we'll fork our process and spin up a copy of the gearman server
    global SERVER_PID
    SERVER_PID = os.fork()
    if not SERVER_PID:
        server = GearmanServer(job_servers[0])
        server.start()
        sys.exit()

    # Create a connection to our server to make sure it works
    connection = GearmanConnection(job_servers[0])
    for i in range(10):
        try:
            connection.connect()
        except ConnectionError:
            time.sleep(0.5)
        else:
            break

    connection.close()

@atexit.register
def stop_server():
    # If we're done running our test, we'll need to gracefully shutdown our GearmanServer
    global SERVER_PID
    if SERVER_PID:
        connection = GearmanConnection(job_servers[0], blocking_timeout=2.0)
        connection.connect()
        connection.send_command(GEARMAN_SERVER_COMMAND_SHUTDOWN, dict(graceful=None))
        connection.close()

if __name__ == '__main__':
    # start_server()
    unittest.main()