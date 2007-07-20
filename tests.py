import unittest, time

from gearman import GearmanClient, GearmanWorker
from gearman.connection import GearmanConnection
from gearman.task import Task

job_servers = ["127.0.0.1"]

def echo(job):
    return job.arg

def fail(job):
    raise Exception()

def sleep(job):
    time.sleep(float(job.arg))
    return job.arg

class TestConnection(unittest.TestCase):
    def setUp(self):
        self.connection = GearmanConnection(job_servers[0])
        self.connection.connect()

    def testNoArgs(self):
        self.connection.send_command_blocking("echo_req")
        cmd = self.connection.recv_blocking()
        self.failUnlessEqual(cmd[0], "echo_res")

    def testWithArgs(self):
        self.connection.send_command_blocking("submit_job", dict(func="echo", uniq="", arg="tea"))
        cmd = self.connection.recv_blocking()
        self.failUnlessEqual(cmd[0], 'job_created')

class TestGearman(unittest.TestCase):
    def setUp(self):
        self.worker = GearmanWorker(job_servers)
        self.worker.register_function("echo", echo)
        self.worker.register_function("fail", echo)
        self.worker.register_function("sleep", echo, timeout=1)
        import thread
        thread.start_new_thread(self.worker.work, tuple()) # TODO: Shouldn't use threads.. but we do for now (also, the thread is never terminated)
        self.client = GearmanClient(job_servers)

    def tearDown(self):
        del self.worker
        del self.client

    def testComplete(self):
        self.failUnlessEqual(self.client.do_task(Task("echo", "bar")), 'bar')

    def testFail(self):
        self.failUnlessRaises(Exception, self.client.do_task(Task("fail", "bar")))

    def testTimeout(self):
        self.failUnlessEqual(self.client.do_task(Task("sleep", "0.1")), '0.1')
        self.failUnlessRaises(Exception, self.client.do_task(Task("sleep", "1.5")))

if __name__ == '__main__':
    unittest.main()
