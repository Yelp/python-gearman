import gearman
from time import sleep

class Worker(gearman.GearmanWorker):
    client_id = None 

    def __init__(self, id, host, port):
        print('initializing worker %d' % id)
        self.client_id = str(id)
        gearman.GearmanWorker.__init__(self, ['%s:%d' % (host, port)])
        
        def task_listener_reverse(gearman_worker, gearman_job):
            print('worker %d: reverse' % id)
            return gearman_job.data[::-1]
        
        def task_listener_long_job(gearman_worker, gearman_job):
            print('worker %d: long_job' % id)
            for i in range(1, 10):
                sleep(1)
                self.send_job_status(gearman_job, i, 10) 
            return gearman_job.data
        
        def task_listener_echo(gearman_worker, gearman_job):
            print('worker %d: echo' % id)
            return gearman_job.data
        
        self.set_client_id(self.client_id)
        self.register_task('reverse', task_listener_reverse)
        self.register_task('long_job', task_listener_long_job)
        self.register_task('echo', task_listener_echo)
