import atexit
import logging
import os, sys, signal, threading
import unittest, time, socket
import collections

from gearman import GearmanClient, GearmanWorker
from gearman.connection import GearmanConnection
from gearman.errors import ConnectionError
from gearman.manager import GearmanManager
from gearman.server import GearmanServer
from gearman.task import Task
from gearman.protocol import *
from gearman.job import GEARMAN_JOB_STATE_PENDING, GEARMAN_JOB_STATE_QUEUED, GEARMAN_JOB_STATE_FAILED, GEARMAN_JOB_STATE_COMPLETE, GEARMAN_JOB_STATE_TIMEOUT
from gearman.constants import BACKGROUND_JOB
from gearman._client_base import GearmanConnectionHandler, GearmanClientBase
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

class GearmanTestCase(unittest.TestCase):
    pass


class MockGearmanClientBase(GearmanClientBase):
    def __init__(self):
        super(MockGearmanClientBase, self).__init__()
        self.command_queues = collections.defaultdict(collections.deque)

    def send_command(self, gearman_connection, cmd_type, cmd_args):
        self.command_queues[gearman_connection].append((cmd_type, cmd_args))

class MockGearmanConnectionHandler(GearmanConnectionHandler):
    def __init__(self, *largs, **kwargs):
        super(MockGearmanConnectionHandler, self).__init__(*largs, **kwargs)
        self.test_sent_command_queue = collections.deque()
        self.test_recv_command_queue = collections.deque()

    def send_command(self, cmd_type, **cmd_args):
        self.test_sent_command_queue.append((cmd_type, cmd_args))
        sent_result = super(MockGearmanConnectionHandler, self).send_command(cmd_type, **cmd_args)
		return sent_result

    def recv_command(self, cmd_type, cmd_args):
        self.test_recv_command_queue.append((cmd_type, cmd_args)) 
        recv_result = super(MockGearmanConnectionHandler, self).recv_command(cmd_type, cmd_args)
        return recv_result

    def recv_noop(self):
        pass

class GearmanConnectionHandlerTest(GearmanTestCase):
    def setUp(self):
        self.client_base = MockGearmanClientBase()
        self.connection_handler = MockGearmanConnectionHandler(self.client_base, None)

    def test_recv_command(self):
        self.connection_handler.recv_command(GEARMAN_COMMAND_NOOP, {})
        self._assert_command_received(GEARMAN_COMMAND_NOOP, {})

        # The mock handler never implemented "recv_all_yours" so we should get an attribute error here
        self.assertRaises(ValueError, self.connection_handler.recv_command, GEARMAN_COMMAND_ALL_YOURS, {})

    def test_send_command(self):
        self.connection_handler.recv_command(GEARMAN_COMMAND_NOOP, {})
        self._assert_command_received(GEARMAN_COMMAND_NOOP, {})

        # The mock handler never implemented "recv_all_yours" so we should get an attribute error here
        self.assertRaises(ValueError, self.connection_handler.recv_command, GEARMAN_COMMAND_ALL_YOURS, {})

    def _assert_command_received(self, expected_cmd_type, expected_cmd_args):
        cmd_type, cmd_args = self.connection_handler.test_recv_command_queue.popleft()
        self.assertEqual(cmd_type, expected_cmd_type)
        self.assertEqual(cmd_args, expected_cmd_args)

    def _assert_command_sent(self, expected_cmd_type, expected_cmd_args):
        cmd_type, cmd_args = self.connection_handler.test_sent_command_queue.popleft()
        self.assertEqual(cmd_type, expected_cmd_type)
        self.assertEqual(cmd_args, expected_cmd_args)

class TestConnection(GearmanTestCase):
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

class TestGearman(GearmanTestCase):
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


class TestManager(GearmanTestCase):
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