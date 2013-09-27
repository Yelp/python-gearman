from testify import *
from multiprocessing import Process
import socket
from time import sleep
from random import choice
#import gearman
from test_worker import Worker
from test_client import Client

class ClientWorker(TestCase):
    port = 4730
    host = 'localhost'
    num_workers = 3
    workers = []
    clients = []

    @class_setup
    def init_system(self):
        # check if there is a gearmand running
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.host, self.port))
        except:
            print('could not connect to port %d' % self.port) 
            raise 

        for i in range(0, self.num_workers):
            self.workers.append(Process(target=self.start_worker, args=(i,)))
            self.workers[i].start()
            self.clients.append(Client(i, self.host, self.port))

    def start_worker(self, id):
        worker = Worker(id, self.host, self.port)
        worker.work()

    def test_submit_job(self):
        message = 'Hello Test!'
        request = choice(self.clients).client_submit_job('reverse', message) 
        assert_equal(message, request.result[::-1]) 
        
    def test_submit_multiple_jobs(self):
        job_list = [{'task':'reverse', 'data':'Hello Test!'}, {'task':'echo', 'data':'Hello Test2!'}]  
        submitted_jobs = choice(self.clients).client_submit_multiple_jobs(job_list, False, True)
        assert_equal(len(submitted_jobs), len(job_list)) 
        assert_equal(job_list[0]['data'], submitted_jobs[0].result[::-1]) 
        assert_equal(job_list[1]['data'], submitted_jobs[1].result) 

    def test_submit_job_null_char(self):
        message = 'Hello\00 Test!'
        request = choice(self.clients).client_submit_job('reverse', message) 
        assert_equal(message, request.result[::-1]) 

    def test_submit_background_job(self):
        message = 'Hello Test!'
        request = choice(self.clients).client_submit_job('reverse', message, None, None, True) 
        assert_equal(request.state, 'CREATED') 
 
    def test_get_background_job_status(self):
        message = 'Hello Test!'
        client = choice(self.clients) 
        request = client.client_submit_job('long_job', message, None, None, True) 
        i = 0
        while not client.get_job_status(request).status['running']:
            sleep(1);
            assert(i < 30)
            i += 1

        while True:
            res = client.get_job_status(request)
            if not res.status['running']:
                break
            sleep(1)
            assert(i < 30)
            i += 1

    @class_teardown
    def kill_workers(self):
        for worker in self.workers:
            worker.terminate()

if __name__ == "__main__":
    run()

