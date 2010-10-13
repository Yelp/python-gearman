import collections
from gearman.constants import PRIORITY_NONE, JOB_UNKNOWN, JOB_PENDING, JOB_CREATED, JOB_FAILED, JOB_COMPLETE

class GearmanJob(object):
    """Represents the basics of a job... used in GearmanClient / GearmanWorker to represent job states"""
    def __init__(self, connection, handle, task, unique, data):
        self.connection = connection
        self.handle = handle

        self.task = task
        self.unique = unique
        self.data = data

    def to_dict(self):
        return dict(task=self.task, job_handle=self.handle, unique=self.unique, data=self.data)

    def __repr__(self):
        return '<GearmanJob connection/handle=(%r, %r), task=%s, unique=%s, data=%r>' % (self.connection, self.handle, self.task, self.unique, self.data)

class GearmanJobRequest(object):
    """Represents a job request... used in GearmanClient to represent job states"""
    def __init__(self, gearman_job, initial_priority=PRIORITY_NONE, background=False, max_attempts=1):
        self.gearman_job = gearman_job

        self.priority = initial_priority
        self.background = background

        self.connection_attempts = 0
        self.max_connection_attempts = max_attempts

        self.initialize_request()

    def initialize_request(self):
        # Holds WORK_COMPLETE responses
        self.result = None

        # Holds WORK_EXCEPTION responses
        self.exception = None

        # Queues to hold WORK_WARNING, WORK_DATA responses
        self.warning_updates = collections.deque()
        self.data_updates = collections.deque()

        # Holds WORK_STATUS / STATUS_REQ responses
        self.status = {}

        self.state = JOB_UNKNOWN
        self.timed_out = False

    def reset(self):
        self.initialize_request()
        self.connection = None
        self.handle = None

    @property
    def status_updates(self):
        """Deprecated since 2.0.1, removing in next major release"""
        output_queue = collections.deque()
        if self.status:
            output_queue.append((self.status.get('numerator', 0), self.status.get('denominator', 0)))

        return output_queue

    @property
    def server_status(self):
        """Deprecated since 2.0.1, removing in next major release"""
        return self.status

    @property
    def job(self):
        return self.gearman_job

    @property
    def complete(self):
        background_complete = bool(self.background and self.state in (JOB_CREATED))
        foreground_complete = bool(not self.background and self.state in (JOB_FAILED, JOB_COMPLETE))

        actually_complete = background_complete or foreground_complete
        return actually_complete

    def __repr__(self):
        formatted_representation = '<GearmanJobRequest task=%r, unique=%r, priority=%r, background=%r, state=%r, timed_out=%r>'
        return formatted_representation % (self.job.task, self.job.unique, self.priority, self.background, self.state, self.timed_out)
