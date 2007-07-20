import unittest, time

from gearman import GearmanClient, GearmanWorker
from gearman.task import Task

job_servers = ["127.0.0.1"]

def echo(job):
    return job.arg

def fail(job):
    raise Exception()

def sleep(job):
    time.sleep(float(job.arg))
    return job.arg

# class TestGearmanClient(unittest.TestCase):
#     def setUp(self):
#         self.client = GearmanClient(job_servers)
# 
#     def tearDown(self):
#         del self.client
# 
#     def testToTask(self):
#         self.failUnlessEqual(self.client.do_task(Task("foo", "bar")), 'You said: "bar"')

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
