from testify import *
from multiprocessing import Process
import socket
from time import sleep
import gearman

class ClientWorker(TestCase):
    port = 4730
    host = 'localhost'
    worker_process = None
    client = None

    @class_setup
    def init_system(self):
        # check if there is a gearmand running
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.host, self.port))
        except:
            print('could not connect to port %d' % self.port) 
            raise 

        self.worker_process = Process(target=self.start_worker)
        self.worker_process.start()

        self.client = gearman.GearmanClient(['%s:%d' % (self.host, self.port)])

    def start_worker(self):
        worker = Worker(self.host, self.port)
        worker.work()

    def test_submit_job(self):
        message = 'Hello Test!'
        request = self.client.submit_job('reverse', message) 
        assert_equal(message, request.result[::-1]) 

    def test_submit_multiple_jobs(self):
        job_list = [{'task':'reverse', 'data':'Hello Test!'}, {'task':'echo', 'data':'Hello Test2!'}]  
        submitted_jobs = self.client.submit_multiple_jobs(job_list, background=False, wait_until_complete=True)
        assert_equal(len(submitted_jobs), len(job_list)) 
        assert_equal(job_list[0]['data'], submitted_jobs[0].result[::-1]) 
        assert_equal(job_list[1]['data'], submitted_jobs[1].result) 

    def test_submit_job_null_char(self):
        message = 'Hello\00 Test!'
        request = self.client.submit_job('reverse', message) 
        assert_equal(message, request.result[::-1]) 

    def test_submit_background_job(self):
        message = 'Hello Test!'
        request = self.client.submit_job('reverse', message, None, None, True) 
        assert_equal(request.state, 'CREATED') 
 
    def test_get_background_job_status(self):
        message = 'Hello Test!'
        request = self.client.submit_job('long_job', message, None, None, True) 
        i = 0
        while not self.client.get_job_status(request).status['running']:
            sleep(1);
            assert(i < 30)
            i += 1

        while True:
            res = self.client.get_job_status(request)
            if not res.status['running']:
                break
            sleep(1)
            assert(i < 30)
            i += 1
    
    @class_teardown
    def kill_worker(self):
        self.worker_process.terminate()

class Worker(gearman.GearmanWorker):
    client_id = 'your_worker_client_id_name'

    def __init__(self, host, port):
        gearman.GearmanWorker.__init__(self, ['%s:%d' % (host, port)])
        
        def task_listener_reverse(gearman_worker, gearman_job):
            return gearman_job.data[::-1]
        
        def task_listener_long_job(gearman_worker, gearman_job):
            for i in range(1, 10):
                sleep(1)
                self.send_job_status(gearman_job, i, 10) 
            return gearman_job.data
        
        def task_listener_echo(gearman_worker, gearman_job):
            return gearman_job.data
        
        self.set_client_id(self.client_id)
        self.register_task('reverse', task_listener_reverse)
        self.register_task('long_job', task_listener_long_job)
        self.register_task('echo', task_listener_echo)

if __name__ == "__main__":
    run()

