import os, sys, signal
import unittest, time, socket

from gearman import GearmanClient, GearmanWorker
from gearman.connection import GearmanConnection
from gearman.manager import GearmanManager
from gearman.server import GearmanServer
from gearman.task import Task

job_servers = ["127.0.0.1"]

class FailedError(Exception):
    pass

def echo(job):
    return job.arg

def fail(job):
    raise FailedError()

def sleep(job):
    time.sleep(float(job.arg))
    return job.arg

class ObjectWorker(object):
    def echo(self, job):
        return job.arg

class ClassWorker(object):
    @staticmethod
    def echo(job):
        return job.arg

class GearmanTestCase(unittest.TestCase):
    def start_server(self):
        self.server_pid = os.fork()
        if not self.server_pid:
            server = GearmanServer()
            server.start()
            sys.exit()
        connection = GearmanConnection(job_servers[0])
        for i in range(10):
            try:
                connection.connect()
            except GearmanConnection.ConnectionError:
                time.sleep(0.5)
            else:
                break
        connection.close()

    def stop_server(self):
        os.kill(self.server_pid, signal.SIGTERM)
        os.waitpid(self.server_pid, 0)

class TestConnection(GearmanTestCase):
    def setUp(self):
        self.start_server()
        self.connection = GearmanConnection(job_servers[0])
        self.connection.connect()

    def tearDown(self):
        self.stop_server()

    def testNoArgs(self):
        self.connection.send_command_blocking("echo_req")
        cmd = self.connection.recv_blocking()
        self.failUnlessEqual(cmd[0], "echo_res")

    def testWithArgs(self):
        self.connection.send_command_blocking("submit_job", dict(func="echo", uniq="", arg="tea"))
        cmd = self.connection.recv_blocking()
        self.failUnlessEqual(cmd[0], 'job_created')

class TestGearman(GearmanTestCase):
    def setUp(self):
        self.start_server()
        self.last_exception = (None, None)
        self.worker = GearmanWorker(job_servers)
        self.worker.register_function("echo", echo)
        self.worker.register_function("fail", fail)
        self.worker.register_function("sleep", sleep, timeout=1)
        self.worker.register_class(ObjectWorker())
        self.worker.register_class(ClassWorker())
        class Hooks(object):
            @staticmethod
            def start(job):
                pass
            @staticmethod
            def complete(job, res):
                pass
            @staticmethod
            def fail(job, exc):
                self.last_exception = (job.func, exc)

        import thread
        self.worker_thread = thread.start_new_thread(self.worker.work, tuple(), dict(hooks=Hooks)) # TODO: Shouldn't use threads.. but we do for now (also, the thread is never terminated)
        self.client = GearmanClient(job_servers)

    def tearDown(self):
        del self.worker
        del self.client
        self.stop_server()

    def testComplete(self):
        self.failUnlessEqual(self.client.do_task(Task("echo", "bar")), 'bar')

    def testFail(self):
        self.failUnlessRaises(self.client.TaskFailed, lambda:self.client.do_task(Task("fail", "bar")))
        # self.failUnlessEqual(self.last_exception[0], "fail")

    def testCompleteAfterFail(self):
        self.failUnlessRaises(self.client.TaskFailed, lambda:self.client.do_task(Task("fail", "bar")))
        self.failUnlessEqual(self.client.do_task(Task("echo", "bar")), 'bar')

    def testTimeout(self):
        self.failUnlessEqual(self.client.do_task(Task("sleep", "0.1")), '0.1')
        self.failUnlessRaises(self.client.TaskFailed, lambda:self.client.do_task(Task("sleep", "1.5")))

    def testCall(self):
        self.failUnlessEqual(self.client("echo", "bar"), 'bar')

    def testObjectWorker(self):
        self.failUnlessEqual(self.client("ObjectWorker.echo", "foo"), "foo")

    def testClassWorker(self):
        self.failUnlessEqual(self.client("ClassWorker.echo", "foo"), "foo")

class TestManager(GearmanTestCase):
    def setUp(self):
        self.start_server()
        self.manager = GearmanManager(job_servers[0])

    def tearDown(self):
        self.stop_server()

    def testStatus(self):
        status = self.manager.status()
        self.failUnless(isinstance(status, dict))

    def testVersion(self):
        version = self.manager.version()
        self.failUnless('.' in version)

    def testWorkers(self):
        workers = self.manager.workers()
        self.failUnless(isinstance(workers, list))

if __name__ == '__main__':
    unittest.main()
