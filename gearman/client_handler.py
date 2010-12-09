import collections
import time
import logging

from gearman.command_handler import GearmanCommandHandler
from gearman.constants import JOB_UNKNOWN, JOB_PENDING, JOB_CREATED, JOB_FAILED, JOB_COMPLETE
from gearman.errors import InvalidClientState
from gearman.protocol import GEARMAN_COMMAND_GET_STATUS, submit_cmd_for_background_priority

gearman_logger = logging.getLogger(__name__)

class GearmanClientCommandHandler(GearmanCommandHandler):
    """Maintains the state of this connection on behalf of a GearmanClient"""
    def __init__(self, connection_manager=None):
        super(GearmanClientCommandHandler, self).__init__(connection_manager=connection_manager)

        # When we first submit jobs, we don't have a handle assigned yet... these handles will be returned in the order of submission
        self.requests_awaiting_handles = collections.deque()
        self.handle_to_request_map = dict()

    ##################################################################
    ##### Public interface methods to be called by GearmanClient #####
    ##################################################################
    def send_job_request(self, current_request):
        """Register a newly created job request"""
        self._assert_request_state(current_request, JOB_UNKNOWN)

        gearman_job = current_request.job

        # Handle the I/O for requesting a job - determine which COMMAND we need to send
        cmd_type = submit_cmd_for_background_priority(current_request.background, current_request.priority)

        outbound_data = self.encode_data(gearman_job.data)
        self.send_command(cmd_type, task=gearman_job.task, unique=gearman_job.unique, data=outbound_data)

        # Once this command is sent, our request needs to wait for a handle
        current_request.state = JOB_PENDING

        self.requests_awaiting_handles.append(current_request)

    def send_get_status_of_job(self, current_request):
        """Forward the status of a job"""
        self._register_request(current_request)
        self.send_command(GEARMAN_COMMAND_GET_STATUS, job_handle=current_request.job.handle)

    def on_io_error(self):
        for pending_request in self.requests_awaiting_handles:
            pending_request.state = JOB_UNKNOWN

        for inflight_request in self.handle_to_request_map.itervalues():
            inflight_request.state = JOB_UNKNOWN

    def _register_request(self, current_request):
        self.handle_to_request_map[current_request.job.handle] = current_request

    def _unregister_request(self, current_request):
        # De-allocate this request for all jobs
        return self.handle_to_request_map.pop(current_request.job.handle, None)

    ##################################################################
    ## Gearman command callbacks with kwargs defined by protocol.py ##
    ##################################################################
    def _assert_request_state(self, current_request, expected_state):
        if current_request.state != expected_state:
            raise InvalidClientState('Expected handle (%s) to be in state %r, got %r' % (current_request.job.handle, expected_state, current_request.state))

    def recv_job_created(self, job_handle):
        if not self.requests_awaiting_handles:
            raise InvalidClientState('Received a job_handle with no pending requests')

        # If our client got a JOB_CREATED, our request now has a server handle
        current_request = self.requests_awaiting_handles.popleft()
        self._assert_request_state(current_request, JOB_PENDING)

        # Update the state of this request
        current_request.job.handle = job_handle
        current_request.state = JOB_CREATED
        self._register_request(current_request)

        return True

    def recv_work_data(self, job_handle, data):
        # Queue a WORK_DATA update
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, JOB_CREATED)

        current_request.data_updates.append(self.decode_data(data))

        return True

    def recv_work_warning(self, job_handle, data):
        # Queue a WORK_WARNING update
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, JOB_CREATED)

        current_request.warning_updates.append(self.decode_data(data))

        return True

    def recv_work_status(self, job_handle, numerator, denominator):
        # Queue a WORK_STATUS update
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, JOB_CREATED)

        # The protocol spec is ambiguous as to what type the numerator and denominator is...
        # But according to Eskil, gearmand interprets these as integers
        current_request.status = {
            'handle': job_handle,
            'known': True,
            'running': True,
            'numerator': int(numerator),
            'denominator': int(denominator),
            'time_received': time.time()
        }
        return True

    def recv_work_complete(self, job_handle, data):
        # Update the state of our request and store our returned result
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, JOB_CREATED)

        current_request.result = self.decode_data(data)
        current_request.state = JOB_COMPLETE
        self._unregister_request(current_request)

        return True

    def recv_work_fail(self, job_handle):
        # Update the state of our request and mark this job as failed
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, JOB_CREATED)

        current_request.state = JOB_FAILED
        self._unregister_request(current_request)

        return True

    def recv_work_exception(self, job_handle, data):
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        current_request = self.handle_to_request_map[job_handle]
        self._assert_request_state(current_request, JOB_CREATED)

        current_request.exception = self.decode_data(data)

        return True

    def recv_status_res(self, job_handle, known, running, numerator, denominator):
        # If we received a STATUS_RES update about this request, update our known status
        current_request = self.handle_to_request_map[job_handle]

        job_known = bool(known == '1')
        # Make our status response Python friendly
        current_request.status = {
            'handle': job_handle,
            'known': job_known,
            'running': bool(running == '1'),
            'numerator': int(numerator),
            'denominator': int(denominator),
            'time_received': time.time()
        }

        # If the server doesn't know about this request, we no longer need to track it
        if not job_known:
            self._unregister_request(current_request)

        return True
