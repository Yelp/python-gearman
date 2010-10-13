:mod:`gearman.job` --- Gearman job definitions
==============================================
.. module:: gearman.job
   :synopsis: Gearman jobs - Common job classes used within each interface

GearmanJob - Basic information about a requested job
----------------------------------------------------
.. autoclass:: GearmanJob

Server identifers
^^^^^^^^^^^^^^^^^
.. attribute:: GearmanJob.connection

    :const:`GearmanConnection` - Server assignment.  Could be :const:`None` prior to client job submission

.. attribute:: GearmanJob.handle

    :const:`string` - Job's server handle.  Handles are NOT interchangeable across different gearman servers 

Job parameters
^^^^^^^^^^^^^^
.. attribute:: GearmanJob.task

    :const:`string` - Job's task

.. attribute:: GearmanJob.unique

    :const:`string` - Job's unique identifier (client assigned)

.. attribute:: GearmanJob.data

    :const:`binary` - Job's binary payload

GearmanJobRequest - State tracker for requested jobs
----------------------------------------------------
.. autoclass:: GearmanJobRequest

Tracking job submission
^^^^^^^^^^^^^^^^^^^^^^^
.. attribute:: GearmanJobRequest.gearman_job

    :const:`GearmanJob` - Job that is being tracked by this :const:`GearmanJobRequest` object

.. attribute:: GearmanJobRequest.priority

* :const:`PRIORITY_NONE` [default]
* :const:`PRIORITY_LOW`
* :const:`PRIORITY_HIGH`

.. attribute:: GearmanJobRequest.background

    :const:`boolean` - Is this job backgrounded?

.. attribute:: GearmanJobRequest.connection_attempts

    :const:`integer` - Number of attempted connection attempts

.. attribute:: GearmanJobRequest.max_connection_attempts

    :const:`integer` - Maximum number of attempted connection attempts before raising an exception

Tracking job progress
^^^^^^^^^^^^^^^^^^^^^
.. attribute:: GearmanJobRequest.result

    :const:`binary` - Job's returned binary payload - Populated if and only if JOB_COMPLETE

.. attribute:: GearmanJobRequest.exception

    :const:`binary` - Job's exception binary payload

.. attribute:: GearmanJobRequest.state

* :const:`JOB_UNKNOWN`  - Request state is currently unknown, either unsubmitted or connection failed
* :const:`JOB_PENDING`  - Request has been submitted, pending handle
* :const:`JOB_CREATED`  - Request has been accepted
* :const:`JOB_FAILED`   - Request received an explicit job failure (job done but errored out)
* :const:`JOB_COMPLETE` - Request received an explicit job completion (job done with results)

.. attribute:: GearmanJobRequest.timed_out

    :const:`boolean` - Did the client hit its polling_timeout prior to a job finishing?

.. attribute:: GearmanJobRequest.complete

    :const:`boolean` - Does the client need to continue to poll for more updates from this job?

Tracking in-flight job updates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Certain GearmanJob's may send back data prior to actually completing.  :const:`GearmanClient` uses these queues to keep track of what/when we received certain updates.

.. attribute:: GearmanJobRequest.warning_updates

    :const:`collections.deque` - Job's warning binary payloads

.. attribute:: GearmanJobRequest.data_updates

    :const:`collections.deque` - Job's data binary payloads

.. attribute:: GearmanJobRequest.status

    :const:`dictionary` - Job's status

    * `handle` - :const:`string` - Job handle
    * `known` - :const:`boolean` - Is the server aware of this request?
    * `running` - :const:`boolean` - Is the request currently being processed by a worker?
    * `numerator` - :const:`integer`
    * `denominator` - :const:`integer`
    * `time_received` - :const:`integer` - Time last updated

.. versionadded:: 2.0.1
   Replaces GearmanJobRequest.status_updates and GearmanJobRquest.server_status

.. attribute:: GearmanJobRequest.status_updates

.. deprecated:: 2.0.1
   Replaced by GearmanJobRequest.status

.. attribute:: GearmanJobRequest.server_status

.. deprecated:: 2.0.1
   Replaced by GearmanJobRequest.status
