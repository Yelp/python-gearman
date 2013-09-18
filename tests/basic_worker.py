import gearman

gm_worker = gearman.GearmanWorker(['localhost:4730'])

#  define method to handled 'reverse' work
def task_listener_reverse(gearman_worker, gearman_job):
  print 'received job: %s' % gearman_job.__dict__
  return gearman_job.data[::-1]

gm_worker.set_client_id('your_worker_client_id_name')
gm_worker.register_task('reverse', task_listener_reverse)

gm_worker.work()
