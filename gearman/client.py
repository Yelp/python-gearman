import collections
import logging
import os
import random
import time

from gearman._connection_manager import GearmanConnectionManager
from gearman.client_handler import GearmanClientCommandHandler
from gearman.constants import FOREGROUND_JOB, BACKGROUND_JOB, NO_PRIORITY, LOW_PRIORITY, HIGH_PRIORITY
from gearman.errors import ServerUnavailable, ConnectionError, InvalidClientState
from gearman.job import GearmanJob, GearmanJobRequest, GEARMAN_JOB_STATE_PENDING

gearman_logger = logging.getLogger('gearman.client')

RANDOM_UNIQUE_BYTES = 8

class GearmanClient(GearmanConnectionManager):
    """
    Public interface that all Gearman API users should see
    """
    command_handler_class = GearmanClientCommandHandler

    def __init__(self, *args, **kwargs):
        # By default we should have non-blocking sockets for a GearmanClient
        kwargs.setdefault('blocking_timeout', 0.0)
        super(GearmanClient, self).__init__(*args, **kwargs)

        # The authoritative copy of all requests that this client knows about
        # Ignores the fact if a request has been bound to a connection or not
        self.request_to_rotating_connection_queue = collections.defaultdict(collections.deque)

    def submit_job(self, function_name, data, unique=None, priority=NO_PRIORITY, background=FOREGROUND_JOB, wait_until_complete=False, timeout=None):
        job_info = dict(function_name=function_name, data=data, unique=unique, priority=priority)
        completed_job_list = self.submit_multiple_jobs([job_info], background=background, timeout=timeout, wait_until_complete=wait_until_complete)
        return completed_job_list[0]

    def submit_multiple_jobs(self, jobs_to_submit, background=FOREGROUND_JOB, wait_until_complete=False, timeout=None):
        """Takes a list of jobs_to_submit with dicts of

        {'function_name': function_name, 'unique': unique, 'data': data}
        """
        assert type(jobs_to_submit) in (list, tuple, set), "Expected multiple jobs, received 1?"

        requests_to_submit = []
        for job_info in jobs_to_submit:
            current_request = self._create_request_from_dictionary(job_info, background=background)
            requests_to_submit.append(current_request)

        return self.submit_multiple_requests(requests_to_submit, wait_until_complete=wait_until_complete, timeout=timeout)

    def submit_multiple_requests(self, job_requests, wait_until_complete=False, timeout=None):
        """Submit these job requests and wait till our jobs are accepted"""
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
        stop_time = timeout and (time.time() + timeout)

        chosen_connections = set()
        for current_request in job_requests:
            # Raise ServerUnavailable error if we could not find a server to connect to
            chosen_conn = self.select_connection_for_request(current_request)
            chosen_connections.add(chosen_conn)

            current_command_handler = self.connection_to_handler_map[chosen_conn]
            current_command_handler.send_job_request(current_request)

        # We should always wait until our job is accepted, this should be fast
        time_remaining = timeout and (stop_time - time.time())
        out_requests = self.wait_until_jobs_accepted(job_requests, timeout=time_remaining)

        # Optionally, we'll allow a user to wait until all jobs are complete with the same timeout
        time_remaining = timeout and (stop_time - time.time())
        if wait_until_complete and (time_remaining is None or time_remaining > 0.0):
            out_requests = self.wait_until_jobs_completed(out_requests, timeout=time_remaining)

        return out_requests

    def wait_until_jobs_accepted(self, job_requests, timeout=None):
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
        job_connections = self._get_connections_from_requests(job_requests)

        is_request_pending = lambda current_request: bool(current_request.state == GEARMAN_JOB_STATE_PENDING)

        # Poll until we know we've gotten acknowledgement that our job's been accepted
        def continue_while_jobs_pending(any_activity, callback_data):
            return any(is_request_pending(current_request) for current_request in job_requests)

        self.poll_connections_until_stopped(job_connections, continue_while_jobs_pending, timeout=timeout)

        # Mark any job still in the queued state to timeout
        for current_request in job_requests:
            current_request.timed_out = is_request_pending(current_request) and bool(not current_request.connection_failed)

        return job_requests

    def wait_until_jobs_completed(self, job_requests, timeout=None):
        """Keep polling on our connection until our job requests are complete"""
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
        job_connections = self._get_connections_from_requests(job_requests)
    
        is_request_incomplete = lambda current_request: not current_request.is_complete()

        # Poll until we get responses for all our functions
        def continue_while_jobs_incomplete(any_activity, callback_data):
            return any(is_request_incomplete(current_request) for current_request in job_requests)

        self.poll_connections_until_stopped(job_connections, continue_while_jobs_incomplete, timeout=timeout)

        # Mark any job still in the queued state to timeout
        for current_request in job_requests:
            job_incomplete = is_request_incomplete(current_request)

            current_request.timed_out = job_incomplete and bool(not current_request.connection_failed)
            if not job_incomplete:
                self.request_to_rotating_connection_queue.pop(current_request, None)

        return job_requests

    def get_job_status(self, current_request, timeout=None):
        request_list = self.get_job_statuses([current_request], timeout=timeout)
        return request_list[0]

    def get_job_statuses(self, job_requests, timeout=None):
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
        for current_request in job_requests:
            current_request.server_status['last_time_received'] = current_request.server_status.get('time_received')

            current_connection = current_request.get_connection()
            current_command_handler = self.connection_to_handler_map[current_connection]

            current_command_handler.send_get_status_of_job(current_request)

        return self.wait_until_job_statuses_received(job_requests, timeout=timeout)

    def wait_until_job_statuses_received(self, job_requests, timeout=None):
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
        job_connections = self._get_connections_from_requests(job_requests)

        is_status_not_updated = lambda current_request: current_request.server_status.get('time_received') == current_request.server_status.get('last_time_received')

        # Poll to make sure we send out our request for a status update
        def continue_while_status_not_updated(any_activity, callback_data):
            return any(is_status_not_updated(current_request) for current_request in job_requests)

        self.poll_connections_until_stopped(job_connections, continue_while_status_not_updated, timeout=timeout)

        for current_request in job_requests:
            current_request.server_status = current_request.server_status or {}
            current_request.timed_out = is_status_not_updated(current_request) and bool(not current_request.connection_failed)

        return job_requests

    def _get_connections_from_requests(self, job_requests):
        submitted_job_connections = set(current_request.get_connection() for current_request in job_requests)
        submitted_job_connections.discard(None)

        return submitted_job_connections

    # Begin helpers for submit_multple_jobs / submit_multiple_requests
    def _create_request_from_dictionary(self, job_info, background):
        """Takes a dictionary with fields  {'function_name': function_name, 'unique': unique, 'data': data, 'priority': priority}"""
        # Make sure we have a unique identifier for ALL our tasks
        job_unique = job_info.get('unique')
        if job_unique == '-':
            job_unique = job_info['data']
        elif not job_unique:
            job_unique = os.urandom(RANDOM_UNIQUE_BYTES).encode('hex')

        current_job = GearmanJob(conn=None, handle=None, function_name=job_info['function_name'], unique=job_unique, data=job_info['data'])
        current_request = GearmanJobRequest(current_job, initial_priority=job_info.get('priority', NO_PRIORITY), background=background)
        return current_request

    def bind_connection_to_request(self, command_handler, current_request):
        current_connection = self.handler_to_connection_map[command_handler]
        current_request.bind_connection(current_connection)

    def select_connection_for_request(self, current_request):
        """Return a live connection for the given hash"""
        if not self.connection_list:
            raise ServerUnavailable('Found no valid connections: %r' % self.connection_list)

        # We'll keep track of the connections we're attempting to use so if we ever have to retry, we can use this history
        rotating_conns = self.request_to_rotating_connection_queue.get(current_request, None)
        if not rotating_conns:
            shuffled_connection_list = list(self.connection_list)
            random.shuffle(shuffled_connection_list)

            rotating_conns = collections.deque(shuffled_connection_list)
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
            raise ServerUnavailable('Found no valid connections: %r' % self.connection_list)

        # Rotate our server list so we'll skip all our broken servers
        rotating_conns.rotate(-skipped_conns)
        return chosen_conn
