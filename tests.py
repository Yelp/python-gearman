import atexit
import logging
import os, sys, signal, threading
import unittest, time, socket

from gearman import GearmanClient, GearmanWorker
from gearman.connection import GearmanConnection, GearmanConnectionError
from gearman.manager import GearmanManager
from gearman.server import GearmanServer
from gearman.task import Task
from gearman.protocol import *
from gearman.job import GEARMAN_JOB_STATE_PENDING, GEARMAN_JOB_STATE_QUEUED, GEARMAN_JOB_STATE_FAILED, GEARMAN_JOB_STATE_COMPLETE

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
        gearman_worker.send_job_status(job, current_status, max_status)

    return max_status

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

    def testNoArgs(self):
        self.connection.send_command(GEARMAN_COMMAND_ECHO_REQ, dict(text=""))
        cmd_tuple = self.connection.recv_command()
        self.failUnless(cmd_tuple)

        cmd_type, cmd_args = cmd_tuple
        self.failUnlessEqual(cmd_type, GEARMAN_COMMAND_ECHO_RES)

    def testWithArgs(self):
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

    def testComplete(self):
        completed_job = self.client.submit_job("echo", "bar")
        self.failUnlessEqual(completed_job.result, 'bar')

    def testFail(self):
        completed_job = self.client.submit_job("fail", "bar")
        self.failUnlessEqual(completed_job.state, GEARMAN_JOB_STATE_FAILED)

    def testCompleteAfterFail(self):
        failed_job = self.client.submit_job("fail", "bar")
        self.failUnlessEqual(failed_job.state, GEARMAN_JOB_STATE_FAILED)

        completed_job = self.client.submit_job("echo", "bar")
        self.failUnlessEqual(completed_job.result, 'bar')

    def testWorkData(self):
        completed_job = self.client.submit_job("work_data", "5")
        self.failUnlessEqual(len(completed_job.data_updates), 5)
        for count in xrange(5):
            self.failUnlessEqual(completed_job.data_updates.popleft(), str(count))

        self.failUnlessEqual(completed_job.result, "5")
    
    def testWorkStatus(self):
        completed_job = self.client.submit_job("work_status", "10")
        self.failUnlessEqual(len(completed_job.status_updates), 10)
        for count in xrange(10):
            self.failUnlessEqual(completed_job.status_updates.popleft(), (str(count), "10"))
    
        self.failUnlessEqual(completed_job.result, "10")
    
    def testWorkWarning(self):
        completed_job = self.client.submit_job("work_warning", "")

        self.failUnlessEqual(len(completed_job.warning_updates), 4)
        self.failUnlessEqual(completed_job.warning_updates.popleft(), "THIS")
        self.failUnlessEqual(completed_job.warning_updates.popleft(), "IS")
        self.failUnlessEqual(completed_job.warning_updates.popleft(), "A")
        self.failUnlessEqual(completed_job.warning_updates.popleft(), "TEST")
        self.failUnlessEqual(completed_job.result, '')

    def testMultipleJobs(self):
        echo_strings = ['foo', 'bar', 'hi', 'there']
        jobs_to_submit = []
        for echo_str in echo_strings:
            jobs_to_submit.append(dict(function_name="echo", data=echo_str, unique=echo_str))

        completed_job_list = self.client.submit_job_list(jobs_to_submit)
        for job_request in completed_job_list:
            self.failUnlessEqual(job_request.gearman_job.data, job_request.result)
            self.failUnlessEqual(job_request.state, GEARMAN_JOB_STATE_COMPLETE)

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
        except GearmanConnectionError:
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
        # connection.send_binary_string("%s\r\n" % GEARMAN_SERVER_COMMAND_SHUTDOWN)
        connection.send_command(GEARMAN_SERVER_COMMAND_SHUTDOWN, dict(graceful=None))
        connection.close()

if __name__ == '__main__':
    start_server()
    unittest.main()