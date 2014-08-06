=====================
Types of Gearman Jobs
=====================

Gearman has two types of jobs, "online" jobs and "offline" jobs. Online jobs
keep a socket open to the gearman server while they run, while offline jobs
disconnect from the gearman server after submitting. As such, offline jobs
place less of a burden on the network and are less vulnerable to flakey network
connections.

===============================     ============    =============
Feature                             Online Rules    Offline Rules
===============================     ============    =============
Supports a return value             yes             no
Supports mid-job data updates       yes             no
Supports mid-job status updates     yes             yes
submit_job(...) blocks              optional        no
===============================     ============    =============

Sample Code
===========

Client::

    import gearman
    import time
    
    
    gm_client = gearman.GearmanClient(['localhost:4730'])
    
    initial_request = gm_client.submit_job(
        "delayed_reverse",         # queue name
        "Hello World!",            # data
        background=False,          # False --> online job, True --> offline job
        wait_until_complete=False  # False --> don't block, True --> block
    )
    
    
    start_time = time.time()
    
    
    def check_job_status(initial_request):
        # Create a request for the gearman server to update the original job's status
        update_job = gearman.job.GearmanJob(gm_client.connection_list[0],
            initial_request.job.handle,
            None,
            None,
            None
        )
        update_request = gearman.job.GearmanJobRequest(update_job)
        update_request.state = 'CREATED'
    
        result = gm_client.get_job_status(update_request)
    
        print ('Time elpased = %.2f, Known = %s, Running = %s, ' +
                'Status = %d/%d, data = %r'
            ) % (
                time.time() - start_time,
                result.status['known'],
                result.status['running'],
                result.status['numerator'],
                result.status['denominator'],
                result.data_updates
        )
    
    for i in xrange(8):
        check_job_status(initial_request)
        time.sleep(1)

Worker::

    import gearman
    import time
    
    
    def delayed_reverse(worker, job):
        start_time = time.time()
        et = lambda: time.time() - start_time
    
        print 'Received job `%s`, will run for 6 seconds' % job.data
    
        time.sleep(2)
        print 'Time elapsed = %.2f, updating status to 1/3' % et()
        worker.send_job_status(job, 1, 3)
    
        time.sleep(1)
        print 'Time elapsed = %.2f, sent half-way data' % et()
        worker.send_job_data(job, 'half way data')
    
        time.sleep(1)
        print 'Time elapsed = %.2f, updating status to 2/3' % et()
        worker.send_job_status(job, 2, 3)
    
        time.sleep(2)
        print 'Time elapsed = %.2f, updating status to 3/3 and returning' % et()
    
        return job.data[::-1]


    gearman_worker = gearman.GearmanWorker(['localhost:4730'])
    gearman_worker.register_task('delayed_reverse', delayed_reverse)
    gearman_worker.work()

Sample Output
=============

Client::

    Time elpased = 0.00, Known = True, Running = False, Status = 0/0, data = deque([])
    Time elpased = 1.00, Known = True, Running = True, Status = 0/0, data = deque([])
    Time elpased = 2.00, Known = True, Running = True, Status = 1/3, data = deque([])
    Time elpased = 3.00, Known = True, Running = True, Status = 1/3, data = deque(['half way data'])
    Time elpased = 4.01, Known = True, Running = True, Status = 2/3, data = deque([])
    Time elpased = 5.01, Known = True, Running = True, Status = 2/3, data = deque([])
    Time elpased = 6.01, Known = True, Running = True, Status = 2/3, data = deque([])
    Time elpased = 7.01, Known = False, Running = False, Status = 0/0, data = deque([])

Worker::

    Received job `Hello World!`, will run for 6 seconds
    Time elapsed = 2.00, updating status to 1/3
    Time elapsed = 3.00, sent half-way data
    Time elapsed = 4.00, updating status to 2/3
    Time elapsed = 6.01, updating status to 3/3 and returning

Note that the client never receives the "3/3" status update -- status and
data updates only work if the client queries for the update while the job
is running. The status/data update features are designed as a mechanism for
checking up on a running job, not a means of returning or persistently storing
job output.
