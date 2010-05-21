import collections
from gearman.errors import ConnectionError

GEARMAN_JOB_STATE_PENDING = 'PENDING'
GEARMAN_JOB_STATE_QUEUED = 'QUEUED'
GEARMAN_JOB_STATE_FAILED = 'FAILED'
GEARMAN_JOB_STATE_COMPLETE = 'COMPLETE'

class GearmanJob(object):
    def __init__(self, conn, handle, task, unique, data):
        self.conn = conn
        self.handle = handle

        self.task = task
        self.unique = unique
        self.data = data

    def connection_handle(self):
        return (self.conn, self.handle)

    def to_dict(self):
        return dict(task=self.task, job_handle=self.handle, unique=self.unique, data=self.data)

    def __repr__(self):
        return '<GearmanJob conn/handle=%r, task=%s, unique=%s, data=%s>' % (self.connection_handle(), self.task, self.unique, self.data)

class GearmanJobRequest(object):
    """Represents a job request... used in GearmanClient and GearmanServer to represent job states"""
    def __init__(self, gearman_job, initial_priority=None, background=False, max_retries=0):
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

        self.state = GEARMAN_JOB_STATE_PENDING
        self.timed_out = False
        self.connection_failed = False

    def reset(self):
        self.initialize_request()
        self.bind_connection(None)
        self.bind_handle(None)

    def bind_connection(self, conn):
        if self.gearman_job.conn is not None and conn is not None:
            raise ConnectionError('Request has already been assigned to connection: %r' % self.gearman_job.conn)

        self.gearman_job.conn = conn

    def bind_handle(self, handle):
        self.gearman_job.handle = handle

    def get_connection(self):
        return self.gearman_job and self.gearman_job.conn

    def get_handle(self):
        return self.gearman_job and self.gearman_job.handle

    def get_job(self):
        return self.gearman_job

    def unique_key(self):
        return self.get_unique_key(self.gearman_job.__dict__)

    def connection_handle(self):
        return self.gearman_job.connection_handle()

    def is_complete(self):
        background_complete = bool(self.background and self.state in (GEARMAN_JOB_STATE_QUEUED))
        foreground_complete = bool(not self.background and self.state in (GEARMAN_JOB_STATE_FAILED, GEARMAN_JOB_STATE_COMPLETE))

        actually_complete = background_complete or foreground_complete
        return actually_complete

    @staticmethod
    def get_unique_key(request_dict):
        """Takes a dictionary with fields 'func' and 'unique' and returns a unique key"""
        return (request_dict['func'], request_dict['unique'])
