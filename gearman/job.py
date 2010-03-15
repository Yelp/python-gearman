import collections

GEARMAN_JOB_STATE_PENDING = "PENDING"
GEARMAN_JOB_STATE_QUEUED = "QUEUED"
GEARMAN_JOB_STATE_FAILED = "FAILED"
GEARMAN_JOB_STATE_COMPLETE = "COMPLETE"

class GearmanJobRequest(object):
    def __init__(self, gearman_job, submit_cmd=None, initial_priority=None, is_background=False, max_retries=0):
        self.gearman_job = gearman_job
        self.submit_cmd = submit_cmd

        self.priority = initial_priority
        self.is_background = is_background

        self.attempted_connections = []

        self.retries_attempted = 0
        self.retries_max = max_retries

        self.initialize_request()

    def bind_connection(self, conn):
        self.gearman_job.conn = conn
        if conn is not None:
            self.attempted_connections.append(conn)

    def bind_handle(self, handle):
        self.gearman_job.handle = handle

    def initialize_request(self):
        self.result = None
        self.exception = None
        self.warning_updates = collections.deque()
        self.data_updates = collections.deque()
        self.status_updates = collections.deque()
        
        self.state = GEARMAN_JOB_STATE_PENDING

    def reset(self):
        self.initialize_request()
        self.bind_connection(None)
        self.bind_handle(None)

    def get_connection(self):
        return self.gearman_job and self.gearman_job.conn

    def get_job(self):
        return self.gearman_job

    def unique_key(self):
        return self.get_unique_key(self.gearman_job.__dict__)

    def connection_handle(self):
        return self.gearman_job.connection_handle()

    def is_complete(self):
        if self.is_background and self.state != GEARMAN_JOB_STATE_QUEUED:
            return False
        elif not self.is_background and self.state not in (GEARMAN_JOB_STATE_FAILED, GEARMAN_JOB_STATE_COMPLETE):
            return False

        return True

    @staticmethod
    def get_unique_key(request_dict):
        """Takes a dictionary with fields 'func' and 'unique' and returns a unique key"""
        return (request_dict['func'], request_dict['unique'])

class GearmanJob(object):
    def __init__(self, conn, handle, function_name, unique, data):
        self.conn = conn
        self.handle = handle

        self.func = function_name
        self.unique = unique
        self.data = data

    def connection_handle(self):
        return (self.conn and self.conn.hostspec, self.handle)

    def __repr__(self):
        return "<GearmanJob conn/handle=%r, func=%s, unique=%s, data=%s>" % (self.connection_handle(), self.func, self.unique, self.data)
