import atexit
import logging
import os, sys, signal, threading
import unittest, time, socket

from gearman import GearmanClient, GearmanWorker
from gearman.connection import GearmanConnection
from gearman.errors import ConnectionError
from gearman.manager import GearmanManager
from gearman.server import GearmanServer
from gearman.task import Task
from gearman.protocol import *
from gearman.job import GEARMAN_JOB_STATE_PENDING, GEARMAN_JOB_STATE_QUEUED, GEARMAN_JOB_STATE_FAILED, GEARMAN_JOB_STATE_COMPLETE, GEARMAN_JOB_STATE_TIMEOUT
from gearman.constants import BACKGROUND_JOB

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
        self.connection.send_command(GEARMAN_COMMAND_SUBMIT_JOB, dict(func="echo", unique="%s" % time.time(), data="tea"))
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

    def tearDown(self):
        del self.worker
        del self.client

    def test_job_complete(self):
        completed_job = self.client.submit_job("echo", "bar")
        self.failUnlessEqual(completed_job.result, 'bar')

    def test_job_failed(self):
        completed_job = self.client.submit_job("fail", "bar")
        self.failUnlessEqual(completed_job.state, GEARMAN_JOB_STATE_FAILED)

    def test_job_timeout(self):
        completed_job = self.client.submit_job("sleep", "0.1", timeout=1.0)
        self.failUnlessEqual(completed_job.state, GEARMAN_JOB_STATE_COMPLETE)
        self.failUnlessEqual(completed_job.result, "0.1")

        completed_job = self.client.submit_job("sleep", "1.5", timeout=1.0)
        self.failUnlessEqual(completed_job.state, GEARMAN_JOB_STATE_TIMEOUT)
        self.failUnlessEqual(completed_job.result, None)

    def test_job_failure_then_complete(self):
        failed_job = self.client.submit_job("fail", "bar")
        self.failUnlessEqual(failed_job.state, GEARMAN_JOB_STATE_FAILED)

        completed_job = self.client.submit_job("echo", "bar")
        self.failUnlessEqual(completed_job.result, 'bar')

    def test_job_data(self):
        completed_job = self.client.submit_job("work_data", "5")
        self.failUnlessEqual(len(completed_job.data_updates), 5)
        for count in xrange(5):
            self.failUnlessEqual(completed_job.data_updates.popleft(), str(count))

        self.failUnlessEqual(completed_job.result, "5")
    
    def test_job_status(self):
        completed_job = self.client.submit_job("work_status", "10")
        self.failUnlessEqual(len(completed_job.status_updates), 10)
        for count in xrange(10):
            self.failUnlessEqual(completed_job.status_updates.popleft(), (float(count), 10.0))

        self.failUnlessEqual(completed_job.result, str(10.0))
    
    def test_job_warning(self):
        completed_job = self.client.submit_job("work_warning", "")

        self.failUnlessEqual(len(completed_job.warning_updates), 4)
        self.failUnlessEqual(completed_job.warning_updates.popleft(), "THIS")
        self.failUnlessEqual(completed_job.warning_updates.popleft(), "IS")
        self.failUnlessEqual(completed_job.warning_updates.popleft(), "A")
        self.failUnlessEqual(completed_job.warning_updates.popleft(), "TEST")
        self.failUnlessEqual(completed_job.result, '')

    def test_multiple_jobs(self):
        echo_strings = ['foo', 'bar', 'hi', 'there']
        jobs_to_submit = []
        for echo_str in echo_strings:
            jobs_to_submit.append(dict(function_name="echo", data=echo_str, unique=echo_str))

        completed_job_list = self.client.submit_job_list(jobs_to_submit)
        for job_request in completed_job_list:
            self.failUnlessEqual(job_request.gearman_job.data, job_request.result)
            self.failUnlessEqual(job_request.state, GEARMAN_JOB_STATE_COMPLETE)

    def test_status_req(self):
        current_job_request = self.client.submit_job("sleep", "0.5", background=BACKGROUND_JOB)
        self.failUnlessEqual(current_job_request.state, GEARMAN_JOB_STATE_QUEUED)
        self.failUnlessEqual(current_job_request.result, None)

        status_response = self.client.get_status(current_job_request, timeout=1.0)
        self.failUnless(status_response['known'])
        self.failUnless(status_response['running'])

        time.sleep(1.0)

        status_response = self.client.get_status(current_job_request, timeout=1.0)
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