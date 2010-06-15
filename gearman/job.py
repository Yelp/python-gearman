import collections
from gearman.constants import PRIORITY_NONE, JOB_PENDING, JOB_QUEUED, JOB_FAILED, JOB_COMPLETE
from gearman.errors import ConnectionError

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
    def __init__(self, gearman_job, initial_priority=PRIORITY_NONE, background=False, max_retries=0):
        self.gearman_job = gearman_job

        self.priority = initial_priority
        self.background = background

        self.retries_attempted = 0
        self.retries_max = max_retries

        self.initialize_request()

    def initialize_request(self):
        # Holds WORK_COMPLETE responses
        self.result = None

        # Holds WORK_EXCEPTION responses
        self.exception = None

        # Queues to hold WORK_WARNING, WORK_DATA, WORK_STATUS responses
        self.warning_updates = collections.deque()
        self.data_updates = collections.deque()
        self.status_updates = collections.deque()

        # Holds STATUS_REQ responses
        self.server_status = {}

        self.state = JOB_PENDING
        self.timed_out = False
        self.connection_failed = False

    def reset(self):
        self.initialize_request()
        self.connection = None
        self.handle = None

    @property
    def job(self):
        return self.gearman_job

    @property
    def complete(self):
        background_complete = bool(self.background and self.state in (JOB_QUEUED))
        foreground_complete = bool(not self.background and self.state in (JOB_FAILED, JOB_COMPLETE))

        actually_complete = background_complete or foreground_complete
        return actually_complete
