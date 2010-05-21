import collections
import time
import logging

from gearman._command_handler import GearmanCommandHandler
from gearman.errors import InvalidClientState
from gearman.job import GEARMAN_JOB_STATE_PENDING, GEARMAN_JOB_STATE_QUEUED, GEARMAN_JOB_STATE_FAILED, GEARMAN_JOB_STATE_COMPLETE
from gearman.protocol import GEARMAN_COMMAND_GET_STATUS, submit_cmd_for_background_priority

gearman_logger = logging.getLogger('gearman.client')

class GearmanClientCommandHandler(GearmanCommandHandler):
    """Maintains the state of this connection on behalf of a GearmanClient"""
    def __init__(self, *largs, **kwargs):
        super(GearmanClientCommandHandler, self).__init__(*largs, **kwargs)
    
        # When we first submit jobs, we don't have a handle assigned yet... these handles will be returned in the order of submission
        self.requests_awaiting_handles = collections.deque()
        self.handle_to_request_map = dict()

    ##################################################################
    ##### Public interface methods to be called by GearmanClient #####
    ##################################################################
    def send_job_request(self, current_request):
        """Register a newly created job request"""
        # Mutate arguments for this job...
        gearman_job = current_request.get_job()

        cmd_type = submit_cmd_for_background_priority(current_request.background, current_request.priority)

        # Handle the IO for requesting a job
        self.send_command(cmd_type, function_name=gearman_job.func, data=gearman_job.data, unique=gearman_job.unique)

        # Once this command is sent, our request is awaiting a handle
        self.requests_awaiting_handles.append(current_request)

    def send_get_status_of_job(self, current_request):
        self.send_command(GEARMAN_COMMAND_GET_STATUS, job_handle=current_request.get_handle())

    def get_requests(self):
        pending_requests = self.requests_awaiting_handles
        inflight_requests = self.handle_to_request_map.itervalues()
        return pending_requests, inflight_requests

    ##################################################################
    ## Gearman command callbacks with kwargs defined by protocol.py ##
    ##################################################################
    def _assert_request_state(self, current_request, expected_state):
        if current_request.state != expected_state:
            raise InvalidClientState('Expected handle (%s) to be in state %r, got %s' % (current_request.get_handle(), expected_state, current_request.state))

    def recv_job_created(self, job_handle):
        if not self.requests_awaiting_handles:
            raise InvalidClientState('Received a job_handle with no pending requests')

        current_request = self.requests_awaiting_handles.popleft()
        self._assert_request_state(current_request, GEARMAN_JOB_STATE_PENDING)

        current_request.bind_handle(job_handle)
        current_request.state = GEARMAN_JOB_STATE_QUEUED

        # Once we know that a job's been created, go ahead ans assign a handle to it
        self.handle_to_request_map[job_handle] = current_request

        return True

    def recv_work_data(self, job_handle, data):
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, GEARMAN_JOB_STATE_QUEUED)

        current_request.data_updates.append(data)

        return True

    def recv_work_warning(self, job_handle, data):
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, GEARMAN_JOB_STATE_QUEUED)

        current_request.warning_updates.append(data)

        return True

    def recv_work_status(self, job_handle, numerator, denominator):
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, GEARMAN_JOB_STATE_QUEUED)

        status_tuple = (float(numerator), float(denominator))
        current_request.status_updates.append(status_tuple)

        return True

    def recv_work_complete(self, job_handle, data):
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, GEARMAN_JOB_STATE_QUEUED)

        current_request.result = data
        current_request.state = GEARMAN_JOB_STATE_COMPLETE

        return True

    def recv_work_fail(self, job_handle):
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, GEARMAN_JOB_STATE_QUEUED)

        current_request.state = GEARMAN_JOB_STATE_FAILED

        return True

    def recv_work_exception(self, job_handle, data):
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, GEARMAN_JOB_STATE_QUEUED)

        current_request.exception = data

        return True

    def recv_status_res(self, job_handle, known, running, numerator, denominator):
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, GEARMAN_JOB_STATE_QUEUED)

        current_request.server_status = {
            'handle': job_handle,
            'known': bool(known == '1'),
            'running': bool(running == '1'),
            'numerator': float(numerator),
            'denominator': float(denominator),
            'time_received': time.time()
        }
        return True
