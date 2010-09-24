:mod:`gearman.worker` --- Gearman worker
========================================
.. module:: gearman.worker
   :synopsis: Gearman worker - public interface for accepting/executing jobs

.. autoclass:: GearmanWorker

Job processing
--------------
.. automethod:: GearmanWorker.set_client_id

.. automethod:: GearmanWorker.register_task

.. automethod:: GearmanWorker.unregister_task

.. automethod:: GearmanWorker.work

Setting up a basic worker that reverses a given byte-string::

    gm_worker = gearman.GearmanWorker(['localhost:4730'])

    # See gearman/job.py to see attributes on the GearmanJob
    # Send back a reversed version of the 'data' string
    def task_listener_reverse(gearman_worker, gearman_job):
        return reversed(gearman_job.data)

    # gm_worker.set_client_id is optional
    gm_worker.set_client_id('your_worker_client_id_name')
    gm_worker.register_task('reverse', task_listener_reverse)

    # Enter our work loop and call gm_worker.after_poll() after each time we timeout/see socket activity
    gm_worker.work()

Sending in-flight job updates
-----------------------------
.. automethod:: GearmanWorker.send_job_data

.. automethod:: GearmanWorker.send_job_status

.. automethod:: GearmanWorker.send_job_warning

Callback function sending back inflight job updates::

    gm_worker = gearman.GearmanWorker(['localhost:4730'])

    # See gearman/job.py to see attributes on the GearmanJob
    # Send back a reversed version of the 'data' string through WORK_DATA instead of WORK_COMPLETE
    def task_listener_reverse_inflight(gearman_worker, gearman_job):
        reversed_data = reversed(gearman_job.data)
        total_chars = len(reversed_data)

        for idx, character in enumerate(reversed_data):
            gearman_worker.send_job_data(gearman_job, str(character))
            gearman_worker.send_job_status(gearman_job, idx + 1, total_chars)

        return None

    # gm_worker.set_client_id is optional
    gm_worker.register_task('reverse', task_listener_reverse_inflight)

    # Enter our work loop and call gm_worker.after_poll() after each time we timeout/see socket activity
    gm_worker.work()

Extending the worker
--------------------
.. autoattribute:: GearmanWorker.data_encoder

.. automethod:: GearmanWorker.after_poll

Send/receive Python objects and do work between polls::

    # By default, GearmanWorker's can only send off byte-strings
    # If we want to be able to send out Python objects, we can specify a data encoder
    # This will automatically convert byte strings <-> Python objects for ALL commands that have the 'data' field
    #
    # See http://gearman.org/index.php?id=protocol for Worker commands that send/receive 'opaque data'
    #
    import json # Or similarly styled library
    class JSONDataEncoder(gearman.DataEncoder):
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
