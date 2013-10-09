from testify import *

from multiprocessing import Process, Queue
from time import sleep
from random import choice
import socket
import subprocess

import gearman
from test_worker import Worker
from test_client import Client

class ClientWorker(TestCase):
    base_port = 14627
    host = 'localhost'
    num_servers = 2
    num_workers = 8
    workers = []
    clients = []
    servers = []
    server_processes = []

    @class_setup
    def init_system(self):
        for i in range(0, self.num_servers):
            port = self.base_port
            self.base_port += 1
            p = subprocess.Popen(['gearmand', '-p', str(port)])
            self.server_processes.append(p)
            started = False
            tries = 0
            while not started:
                # check if there is a gearmand running
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                #print('connecting to: %s:%d' % (self.host, port))
                try:
                    s.connect((self.host, port))
                    started = True
                    self.servers.append('%s:%d' % (self.host, port))
                except:
                    tries += 1
                    if tries > 5:
                        raise
                    else:
                        sleep(1)
                        pass

        for i in range(0, self.num_workers):
            self.workers.append(Process(target=self.start_worker, args=(i,self.log)))
            self.workers[i].start()
            self.clients.append(Client(i, self.servers))

    def start_worker(self, id, log):
        worker = Worker(id, self.servers, log)
        worker.work()

    def test_submit_job(self):
        ''' Submit a single job and block until it is complete '''
        message = 'Hello Test!'
        request = choice(self.clients).client_submit_job('reverse', message)
        assert_equal(request.priority, gearman.PRIORITY_NONE)
        assert_equal(message, request.result[::-1])

    def test_submit_job_high_priority(self):
        ''' Submit a single job with high priority and block until it is complete '''
        message = 'Hello Test!'
        request = choice(self.clients).client_submit_job('reverse', message, None, gearman.PRIORITY_HIGH)
        assert_equal(request.priority, gearman.PRIORITY_HIGH)
        assert_equal(message, request.result[::-1])

    def test_submit_job_low_priority(self):
        ''' Submit a single job with low priority and block until it is complete '''
        message = 'Hello Test!'
        request = choice(self.clients).client_submit_job('reverse', message, None, gearman.PRIORITY_LOW)
        assert_equal(request.priority, gearman.PRIORITY_LOW)
        assert_equal(message, request.result[::-1])

    def test_submit_multiple_jobs(self):
        ''' Submit multiple jobs and block until all are complete '''
        job_list = [{'task':'reverse', 'data':'Hello Test!'}, {'task':'echo', 'data':'Hello Test2!'}]
        submitted_jobs = choice(self.clients).client_submit_multiple_jobs(job_list, False, True)
        assert_equal(len(submitted_jobs), len(job_list))
        assert_equal(job_list[0]['data'], submitted_jobs[0].result[::-1])
        assert_equal(job_list[1]['data'], submitted_jobs[1].result)

    def test_submit_job_null_char(self):
        ''' Submit a job with a null character '''
        message = 'Hello\00 Test!'
        request = choice(self.clients).client_submit_job('reverse', message)
        assert_equal(message, request.result[::-1])

    def test_submit_background_job(self):
        ''' Submit a background job '''
        message = 'Hello Test!'
        request = choice(self.clients).client_submit_job('reverse', message, None, gearman.PRIORITY_NONE, True)
        assert_equal(request.priority, gearman.PRIORITY_NONE)
        assert_equal(request.state, 'CREATED')

    def test_submit_background_job_high_priority(self):
        ''' Submit a background job '''
        message = 'Hello Test!'
        request = choice(self.clients).client_submit_job('reverse', message, None, gearman.PRIORITY_HIGH, True)
        assert_equal(request.priority, gearman.PRIORITY_HIGH)
        assert_equal(request.state, 'CREATED')

    def test_submit_background_job_low_priority(self):
        ''' Submit a background job '''
        message = 'Hello Test!'
        request = choice(self.clients).client_submit_job('reverse', message, None, gearman.PRIORITY_LOW, True)
        assert_equal(request.priority, gearman.PRIORITY_LOW)
        assert_equal(request.state, 'CREATED')

    def test_get_background_job_status(self):
        ''' Submit a background job and check to make sure it gets run '''
        message = 'Hello Test!'
        client = choice(self.clients)
        request = client.client_submit_job('long_job', message, None, None, True)
        i = 0

        # wait until the job has started - running=False -> running=True
        while not client.get_job_status(request).status['running']:
            sleep(1);
            assert(i < 30)
            i += 1

        # wait until the job has finished - running=True -> running=False
        while True:
            res = client.get_job_status(request)
            if not res.status['running']:
                break
            sleep(1)
            assert(i < 30)
            i += 1

    def test_stress_workers(self):
        num_jobs = 50000#0#0
        stress_fns = ['stress']#, 'stress1', 'stress2']
        for i in range(0, num_jobs):
            choice(self.clients).client_submit_job(choice(stress_fns), '', None, gearman.PRIORITY_NONE, True)
            if i % 1000 == 0:
                print 100 * float(i) / float(num_jobs)

    def workers_complete(self, workers):
        for worker in workers:
            if worker.is_alive():
                return False
        return True

    @class_teardown
    def kill_workers(self):
        for server in self.servers:
            while self.server_has_queued_jobs(server):
                sleep(0.5)

        for worker in self.workers:
            worker.terminate()
        for server in self.server_processes:
            server.kill()

        with open('/tmp/python-gearman.csv', 'w') as outfile:
            for i in range(0, self.num_workers):
                with open('/tmp/worker%d' % i) as infile:
                    outfile.write(infile.read())

    def server_has_queued_jobs(self, server):
        admin_client = gearman.GearmanAdminClient([server])
        for task_status in admin_client.get_status():
            if task_status['queued'] > 0 or task_status['running'] != 0:
                return True
        return False

if __name__ == "__main__":
    run()

