#!/usr/bin/env python
# TODO: Implement retry behavior
# TODO: Test rotating servers

import collections
import time
import uuid
import random
import logging

import gearman.util
from gearman._client_base import GearmanClientBase, GearmanConnectionHandler
from gearman.errors import ServerUnavailable, ConnectionError
from gearman.compat import *
from gearman.connection import GearmanConnection
from gearman.protocol import *
from gearman.job import GearmanJob, GearmanJobRequest, GEARMAN_JOB_STATE_PENDING, GEARMAN_JOB_STATE_QUEUED, GEARMAN_JOB_STATE_FAILED, GEARMAN_JOB_STATE_COMPLETE, GEARMAN_JOB_STATE_TIMEOUT
from gearman.constants import FOREGROUND_JOB, BACKGROUND_JOB, NO_PRIORITY, LOW_PRIORITY, HIGH_PRIORITY

gearman_logger = logging.getLogger("gearman.client")

class GearmanClient(GearmanClientBase):
    client_type = "client"

    def __init__(self, *args, **kwargs):
        # By default we should have non-blocking sockets for a GearmanClient
        kwargs.setdefault('blocking_timeout', 0.0)
        kwargs.setdefault('connection_handler_class', GearmanClientConnectionHandler)
        super(GearmanClient, self).__init__(*args, **kwargs)

        # The authoritative copy of all requests that this client knows about
        # Ignores the fact if a request has been bound to a connection or not
        self.request_to_rotating_connection_queue = collections.defaultdict(collections.deque)

    def submit_job(self, function_name, data, unique=None, priority=NO_PRIORITY, background=FOREGROUND_JOB):
        job_info = dict(function_name=function_name, data=data, unique=unique)
        completed_job_list = self.submit_job_list([job_info], priority=priority, background=background)
        return completed_job_list[0]

    def submit_job_list(self, jobs_to_submit, priority=NO_PRIORITY, background=FOREGROUND_JOB):
        """Takes a list of jobs_to_submit with dicts of
        
        {'function_name': function_name, 'unique': unique, 'data': data}
        """
        submitted_job_requests = []
        for job_info in jobs_to_submit:
            current_request = self._create_request_from_dictionary(job_info, priority=priority, background=background)

            chosen_conn = self._choose_connection_for_request(current_request)

            current_connection_handler = self.connection_handlers[chosen_conn]
            current_connection_handler.send_job_request(current_request)

            submitted_job_requests.append(current_request)

        return submitted_job_requests

    def _create_request_from_dictionary(self, job_info, priority, background):
       """Takes a dictionary with fields  {'function_name': function_name, 'unique': unique, 'data': data}"""
       # Make sure we have a unique identifier for ALL our tasks
       job_unique = job_info.get('unique')
       if job_unique == '-':
           job_unique = job_info['data']
       elif not job_unique:
           job_unique = uuid.uuid4().hex

       current_job = GearmanJob(conn=None, handle=None, function_name=job_info['function_name'], unique=job_unique, data=job_info['data'])
       current_request = GearmanJobRequest(current_job, initial_priority=priority, is_background=background)
       return current_request

    def _choose_connection_for_request(self, current_request):
        """Return a live connection for the given hash"""
        rotating_conns = self.request_to_rotating_connection_queue.get(current_request, None)
        if not rotating_conns:
            initial_rotation = hash(current_request) % len(self.connection_list)
            rotating_conns = collections.deque(self.connection_list)

            # Rotate left
            rotating_conns.rotate(-initial_rotation)
            self.request_to_rotating_connection_queue[current_request] = rotating_conns

        chosen_conn = None
        skipped_conns = 0

        for possible_conn in rotating_conns:
            try:
                possible_conn.connect() # Make sure the connection is up (noop if already connected)
                chosen_conn = possible_conn
                break
            except ConnectionError:
                skipped_conns += 1

        if not chosen_conn:
            raise ServerUnavailable("Unable to Locate Server")

        # Rotate our server list so we'll skip all our broken servers
        rotating_conns.rotate(-skipped_conns)
        return chosen_conn

    def wait_for_job_completion(self, submitted_job_requests, timeout=None):
        """Keep polling on our connection until our job requests are complete"""
        submitted_job_connections = set(current_request.get_connection() for current_request in submitted_job_requests)
        submitted_job_connections.discard(None)

        # Continue to poll while our jobs are not complete
        def callback_on_possible_job_completion(self, any_activity):
            return not all(job_request.is_complete() for job_request in submitted_job_requests)

        self.poll_connections_until_stopped(submitted_job_connections, callback_on_possible_job_completion, timeout=timeout)

        # Mark any job still in the queued state to timeout
        for current_request in submitted_job_requests:
            if not current_request.is_complete():
                current_request.state = GEARMAN_JOB_STATE_TIMEOUT

        return submitted_job_requests

    def get_status(self, current_request, timeout=None):
        last_status_time = current_request.server_status.get('time_received')

        current_connection = current_request.get_connection()
        current_connection_handler = self.connection_handlers[current_connection]

        current_connection_handler.send_get_status_of_job(current_request)

        # Continue to poll while our time_received is the same...
        def callback_on_possible_status_received(self, any_activity):
            return bool(current_request.server_status.get('time_received') == last_status_time)

        self.poll_connections_until_stopped([current_connection], callback_on_possible_status_received, timeout=timeout)

        return current_request.server_status

class GearmanClientConnectionHandler(GearmanConnectionHandler):
    """Command handler is the state machine for this client"""
    def __init__(self, *largs, **kwargs):
        super(GearmanClientConnectionHandler, self).__init__(*largs, **kwargs)

        # When we first submit jobs, we don't have a handle assigned yet... these handles will be returned in the order of submission
        self.requests_awaiting_handles = collections.deque()
        self.handle_to_request_map = dict()

    ###########################################################################
    def send_job_request(self, current_request):
        """Register a newly created job request"""
        current_request.bind_connection(self.gearman_connection)

        # Mutate arguments for this job...
        gearman_job = current_request.get_job()

        cmd_type = submit_cmd_for_background_priority(current_request.is_background, current_request.priority)

        # Handle the IO for requesting a job
        self.send_command(cmd_type, function_name=gearman_job.func, data=gearman_job.data, unique=gearman_job.unique)

        # Once this command is sent, our request is awaiting a handle
        self.requests_awaiting_handles.append(current_request)

    def send_get_status_of_job(self, current_request):
        current_handle = current_request.get_handle()
        self.send_command(GEARMAN_COMMAND_GET_STATUS, job_handle=current_handle)

    ###########################################################################
    # Command callbacks deal with keyword arguments as defined in protocol.py #
    ###########################################################################
    def recv_job_created(self, job_handle):
        current_request = self.requests_awaiting_handles.popleft()
        current_request.bind_handle(job_handle)
        current_request.state = GEARMAN_JOB_STATE_QUEUED

        # Once we know that a job's been created, go ahead ans assign a handle to it
        self.handle_to_request_map[job_handle] = current_request

        return True

    def recv_work_data(self, job_handle, data):
        current_request = self.handle_to_request_map[job_handle]
        current_request.data_updates.append(data)

        return True

    def recv_work_warning(self, job_handle, data):
        current_request = self.handle_to_request_map[job_handle]
        current_request.warning_updates.append(data)

        return True

    def recv_work_status(self, job_handle, numerator, denominator):
        current_request = self.handle_to_request_map[job_handle]

        status_tuple = (float(numerator), float(denominator))
        current_request.status_updates.append(status_tuple)

        return True

    def recv_work_complete(self, job_handle, data):
        current_request = self.handle_to_request_map[job_handle]
        current_request.result = data
        current_request.state = GEARMAN_JOB_STATE_COMPLETE

        return True

    def recv_work_fail(self, job_handle):
        current_request = self.handle_to_request_map[job_handle]
        current_request.state = GEARMAN_JOB_STATE_FAILED

        return True

    def recv_work_exception(self, job_handle, data):
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        current_request = self.handle_to_request_map[job_handle]
        current_request.exception = data

        return True

    def recv_error(self, error_code, error_text):
        gearman_logger.error("Error from server: %s: %s" % (err_code, err_text))
        self.client_base.handle_error(self.gearman_connection)

        return False

    def recv_status_res(self, job_handle, known, running, numerator, denominator):
        current_request = self.handle_to_request_map[job_handle]
        current_request.server_status = {
            'handle': job_handle,
            'known': bool(known == '1'),
            'running': bool(running == '1'),
            'numerator': float(numerator),
            'denominator': float(denominator),
            'time_received': time.time()
        }

        return True
