==============
python-gearman
==============

Description
===========
Python Gearman API - Client, worker, and admin client interfaces

For information on the Gearman protocol and a Gearman server, see http://www.gearman.org/

Below are some code samples to help you get started with the API.

Function available to all examples
----------------------------------
::

    def check_request_status(job_request)
        if job_request.complete:
            print "Job %s finished!  Result: %s - %s" % (job_request.job.unique, job_request.state, job_request.result)
        elif job_request.timed_out:
            print "Job %s timed out!" % job_request.unique
        elif job_request.connection_failed:
            print "Job %s connection failed!" % job_request.unique

Client Examples
===============
1) Single job - blocking
------------------------
::

    gm_client = gearman.GearmanClient(['localhost:4730', 'otherhost:4730'])
    
    # See gearman/job.py to see attributes on the GearmanJobRequest
    # Defaults to PRIORITY_NONE, background=False (synchronous task), wait_until_complete=False
    completed_job_request = gm_client.submit_job("task_name", "arbitrary binary data", wait_until_complete=True)
    check_request_status(completed_job_request)

2) Single job - high priority, background, blocking
---------------------------------------------------
::

    gm_client = gearman.GearmanClient(['localhost:4730', 'otherhost:4730'])
    
    # See gearman/job.py to see attributes on the GearmanJobRequest
    submitted_job_request = gm_client.submit_job("task_name", "arbitrary binary data", priority=gearman.PRIORITY_HIGH, background=True, wait_until_complete=True)
    
    check_request_status(submitted_job_request)

3) Multiple jobs - non-blocking (wait_until_complete=False)
-----------------------------------------------------------
::

    import time
    gm_client = gearman.GearmanClient(['localhost:4730'])
    
    list_of_jobs = [dict(task="task_name", data="binary data"), dict(task="other_task", data="other binary data")]
    submitted_requests = gm_client.submit_multiple_jobs(list_of_jobs, background=False)
    
    # Once we know our jobs are accepted, we can do other stuff and wait for results later in the function
    # Similar to multithreading and doing a join except this is all done in a single process
    time.sleep(1.0)
    
    # Wait at most 3 seconds before timing out incomplete requests
    completed_requests = gm_client.wait_until_jobs_completed(submitted_requests, timeout=5.0)
    for completed_job_request in completed_results:
        check_request_status(completed_job_request)

4) Recovering from failed connections
-------------------------------------
::

    import time
    gm_client = gearman.GearmanClient(['localhost:4730'])
    
    list_of_jobs = [dict(task="task_name", data="task binary string"), dict(task="other_task", data="other binary string")]
    failed_requests = gm_client.submit_multiple_jobs(list_of_jobs, background=False)
    
    # Let's pretend our assigned requests' Gearman servers all failed
    assert all(request.connection_failed for request in failed_requests), "All connections didn't fail!"
    
    # Let's pretend our assigned requests' don't fail but some simply timeout
    retried_connection_failed_requests = gm_client.submit_multiple_requests(failed_requests, wait_until_complete=True, timeout=1.0)
    
    timed_out_requests = [job_request for job_request in retried_requests if job_request.timed_out]
    
    # For our timed out requests, lets wait a little longer until they're complete
    retried_timed_out_requests = gm_client.submit_multiple_requests(timed_out_requests, wait_until_complete=True, timeout=4.0)

5) Extending the client with a different data encoder and logging options
-------------------------------------------------------------------------
::

    import cPickle as pickle
    
    class PickleDataEncoder(gearman.DataEncoder)
        @classmethod
        def encode(cls, encodable_object):
            return pickle.dumps(encodable_object)
    
        @classmethod
        def decode(cls, decodable_string):
            return pickle.loads(decodable_string)
    
    class LoggingPickleExampleClient(gearman.GearmanClient):
        data_encoder = PickleDataEncoder
    
        def wait_until_jobs_completed(self, job_requests, timeout=None):
            self.log.info("Waiting for jobs to complete...")
            output_results = super(LoggingPickleExampleClient, self).wait_until_jobs_completed(job_requests, timeout=timeout)
            self.log.info("Finished waiting for jobs to complete...")
            return output_results

Worker Examples
===============
1) Listening for a 'reverse' task and returning the result
----------------------------------------------------------
::

    gm_worker = gearman.GearmanWorker(['localhost:4730'])
    
    # See gearman/job.py to see attributes on the GearmanJob
    # Send back a reversed version of the 'data' string
    def task_listener_reverse(gearman_job):
        return reversed(gearman_job.data)
    
    # gm_worker.set_client_id is optional
    gm_worker.set_client_id('your_worker_client_id_name')
    gm_worker.register_task('reverse', task_listener_reverse)
    
    # Enter our work loop and call gm_worker.after_poll() after each time we timeout/see socket activity
    gm_worker.work()

2) Extending the worker with a different data encoder and polling behavior
--------------------------------------------------------------------------
::

    import json # Or similarly styled library
    class JSONDataEncoder(gearman.DataEncoder)
        @classmethod
        def encode(cls, encodable_object):
            return json.dumps(encodable_object)
    
        @classmethod
        def decode(cls, decodable_string):
            return json.loads(decodable_string)
    
    class DBRollbackJSONWorker(gearman.GearmanWorker):
        data_encoder = JSONDataEncoder
    
        def after_poll(self, any_activity):
            # After every select loop, let's rollback our DB connections just to be safe
            continue_working = True
            self.db_connections.rollback()
            return continue_working

Admin Client Examples
=====================
1) Checking in on a single host
-------------------------------
::

    gm_admin_client = gearman.GearmanAdminClient(['localhost:4730'])
    
    status_response = gm_admin_client.get_status()
    version_response = gm_admin_client.get_version()
    workers_response = gm_admin_client.get_workers()
