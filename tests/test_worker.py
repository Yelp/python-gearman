import gearman
from time import sleep
from datetime import datetime

class Worker(gearman.GearmanWorker):
    id = None 
    start_time = None

    def __init__(self, id, servers):
        self.id = id
        #print('initializing worker %d' % self.id)
        self.start_time = datetime.now()
        gearman.GearmanWorker.__init__(self, servers)
        
        def task_listener_reverse(gearman_worker, gearman_job):
            #print('worker %d: reverse' % self.id)
            return gearman_job.data[::-1]
        
        def task_listener_long_job(gearman_worker, gearman_job):
            #print('worker %d: long_job' % self.id)
            long_job_time = 5
            for i in range(1, long_job_time):
                sleep(1)
                self.send_job_status(gearman_job, i, long_job_time) 
            return gearman_job.data
        
        def task_listener_echo(gearman_worker, gearman_job):
            #print('worker %d: echo' % self.id)
            return gearman_job.data
        
        def task_listener_stress(gearman_worker, gearman_job):
            #td = datetime.now() - self.start_time
            #print('%s,%d%d%d' % (int(td.total_seconds()), self.id, self.id, self.id))
            sleep(0.01)
            return gearman_job.data

        self.set_client_id(str(self.id))
        self.register_task('reverse', task_listener_reverse)
        self.register_task('long_job', task_listener_long_job)
        self.register_task('echo', task_listener_echo)
        self.register_task('stress', task_listener_stress)
