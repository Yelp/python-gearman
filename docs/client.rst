:mod:`gearman.client` --- Gearman client
==========================================
.. module:: gearman.client
   :synopsis: Gearman client - public interface for requesting jobs

Function available to all examples::

    def check_request_status(job_request):
        if job_request.complete:
            print "Job %s finished!  Result: %s - %s" % (job_request.job.unique, job_request.state, job_request.result)
        elif job_request.timed_out:
            print "Job %s timed out!" % job_request.unique
        elif job_request.state == JOB_UNKNOWN:
            print "Job %s connection failed!" % job_request.unique

.. autoclass:: GearmanClient

Submitting jobs
---------------
.. automethod:: GearmanClient.submit_job

    Sending a simple job as a blocking call::

        gm_client = gearman.GearmanClient(['localhost:4730', 'otherhost:4730'])

        # See gearman/job.py to see attributes on the GearmanJobRequest
        # Defaults to PRIORITY_NONE, background=False (synchronous task), wait_until_complete=True
        completed_job_request = gm_client.submit_job("task_name", "arbitrary binary data")
        check_request_status(completed_job_request)

    Sending a high priority, background, blocking call::

       gm_client = gearman.GearmanClient(['localhost:4730', 'otherhost:4730'])

       # See gearman/job.py to see attributes on the GearmanJobRequest
       submitted_job_request = gm_client.submit_job("task_name", "arbitrary binary data", priority=gearman.PRIORITY_HIGH, background=True)

       check_request_status(submitted_job_request)


.. automethod:: GearmanClient.submit_multiple_jobs

    Sending multiple jobs all at once and behave like a non-blocking call (wait_until_complete=False)::

        import time
        gm_client = gearman.GearmanClient(['localhost:4730'])

        list_of_jobs = [dict(task="task_name", data="binary data"), dict(task="other_task", data="other binary data")]
        submitted_requests = gm_client.submit_multiple_jobs(list_of_jobs, background=False, wait_until_complete=False)

        # Once we know our jobs are accepted, we can do other stuff and wait for results later in the function
        # Similar to multithreading and doing a join except this is all done in a single process
        time.sleep(1.0)

        # Wait at most 5 seconds before timing out incomplete requests
        completed_requests = gm_client.wait_until_jobs_completed(submitted_requests, poll_timeout=5.0)
        for completed_job_request in completed_requests:
            check_request_status(completed_job_request)


.. automethod:: GearmanClient.submit_multiple_requests

    Recovering from failed connections::

        import time
        gm_client = gearman.GearmanClient(['localhost:4730'])

        list_of_jobs = [dict(task="task_name", data="task binary string"), dict(task="other_task", data="other binary string")]
        failed_requests = gm_client.submit_multiple_jobs(list_of_jobs, background=False)

        # Let's pretend our assigned requests' Gearman servers all failed
        assert all(request.state == JOB_UNKNOWN for request in failed_requests), "All connections didn't fail!"

        # Let's pretend our assigned requests' don't fail but some simply timeout
        retried_connection_failed_requests = gm_client.submit_multiple_requests(failed_requests, wait_until_complete=True, poll_timeout=1.0)

        timed_out_requests = [job_request for job_request in retried_requests if job_request.timed_out]

        # For our timed out requests, lets wait a little longer until they're complete
        retried_timed_out_requests = gm_client.submit_multiple_requests(timed_out_requests, wait_until_complete=True, poll_timeout=4.0)

.. automethod:: GearmanClient.wait_until_jobs_accepted

.. automethod:: GearmanClient.wait_until_jobs_completed

Retrieving job status
---------------------
.. automethod:: GearmanClient.get_job_status

.. automethod:: GearmanClient.get_job_statuses

Extending the client
--------------------
.. autoattribute:: GearmanClient.data_encoder

Send/receive Python objects (not just byte strings)::

    # By default, GearmanClient's can only send off byte-strings
    # If we want to be able to send out Python objects, we can specify a data encoder
    # This will automatically convert byte strings <-> Python objects for ALL commands that have the 'data' field
    #
    # See http://gearman.org/index.php?id=protocol for client commands that send/receive 'opaque data'
    import pickle

    class PickleDataEncoder(gearman.DataEncoder):
        @classmethod
        def encode(cls, encodable_object):
            return pickle.dumps(encodable_object)

        @classmethod
        def decode(cls, decodable_string):
            return pickle.loads(decodable_string)

    class PickleExampleClient(gearman.GearmanClient):
        data_encoder = PickleDataEncoder

    my_python_object = {'hello': 'there'}

    gm_client = PickleExampleClient(['localhost:4730'])
    gm_client.submit_job("task_name", my_python_object)
