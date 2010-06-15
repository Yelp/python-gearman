import collections
import logging
import os
import random
import time

import gearman.util

from gearman.connection_manager import GearmanConnectionManager
from gearman.client_handler import GearmanClientCommandHandler
from gearman.constants import PRIORITY_NONE, PRIORITY_LOW, PRIORITY_HIGH, JOB_PENDING
from gearman.errors import ServerUnavailable, ConnectionError, InvalidClientState
from gearman.job import GearmanJob, GearmanJobRequest

gearman_logger = logging.getLogger(__name__)

RANDOM_UNIQUE_BYTES = 8

class GearmanClient(GearmanConnectionManager):
    """
    GearmanClient :: Interface to submit jobs to a Gearman server

    Submits a single/multiple jobs to a gearman server
    """
    command_handler_class = GearmanClientCommandHandler

    def __init__(self, host_list=None):
        super(GearmanClient, self).__init__(host_list=host_list)

        # The authoritative copy of all requests that this client knows about
        # Ignores the fact if a request has been bound to a connection or not
        self.request_to_rotating_connection_queue = collections.defaultdict(collections.deque)

    def submit_job(self, task, data, unique=None, priority=PRIORITY_NONE, background=False, wait_until_complete=False, timeout=None):
        """Submit a single job to any gearman server"""
        job_info = dict(task=task, data=data, unique=unique, priority=priority)
        completed_job_list = self.submit_multiple_jobs([job_info], background=background, wait_until_complete=wait_until_complete, timeout=timeout)
        return gearman.util.unlist(completed_job_list)

    def submit_multiple_jobs(self, jobs_to_submit, background=False, wait_until_complete=False, timeout=None):
        """Takes a list of jobs_to_submit with dicts of

        {'task': task, 'data': data, 'unique': unique, 'priority': priority}
        """
        assert type(jobs_to_submit) in (list, tuple, set), "Expected multiple jobs, received 1?"

        # Convert all job dicts to job request objects
        requests_to_submit = [self._create_request_from_dictionary(job_info, background=background) for job_info in jobs_to_submit]

        return self.submit_multiple_requests(requests_to_submit, wait_until_complete=wait_until_complete, timeout=timeout)

    def submit_multiple_requests(self, job_requests, wait_until_complete=False, timeout=None):
        """Take GearmanJobRequests, assign them connections, and request that they be done.

        * Blocks until our jobs are accepted (should be fast) OR times out
        * Optionally blocks until jobs are all complete

        You MUST check the status of your requests after calling this function as "timed_out" or "connection_failed" maybe True
        """
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
        stopwatch = gearman.util.Stopwatch(timeout)

        chosen_connections = set()
        for current_request in job_requests:
            chosen_conn = self._choose_request_connection(current_request)
            chosen_connections.add(chosen_conn)

            current_request.job.connection = chosen_conn
            current_request.connection_failed = False
            current_request.timed_out = False

            current_command_handler = self.connection_to_handler_map[chosen_conn]
            current_command_handler.send_job_request(current_request)

        # We should always wait until our job is accepted, this should be fast
        time_remaining = stopwatch.get_time_remaining()
        out_requests = self.wait_until_jobs_accepted(job_requests, timeout=time_remaining)

        # Optionally, we'll allow a user to wait until all jobs are complete with the same timeout
        time_remaining = stopwatch.get_time_remaining()
        if wait_until_complete and bool(time_remaining != 0.0):
            out_requests = self.wait_until_jobs_completed(out_requests, timeout=time_remaining)

        return out_requests

    def wait_until_jobs_accepted(self, job_requests, timeout=None):
        """Go into a select loop until all our jobs have moved to STATE_PENDING"""
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
        job_connections = self._get_request_connections(job_requests)

        def is_request_pending(current_request):
            return bool(current_request.state == JOB_PENDING)

        # Poll until we know we've gotten acknowledgement that our job's been accepted
        def continue_while_jobs_pending(any_activity):
            return any(bool(is_request_pending(current_request) and not current_request.connection_failed) for current_request in job_requests)

        self.poll_connections_until_stopped(job_connections, continue_while_jobs_pending, timeout=timeout)

        # Mark any job still in the queued state to timeout
        for current_request in job_requests:
            current_request.timed_out = is_request_pending(current_request)

        return job_requests

    def wait_until_jobs_completed(self, job_requests, timeout=None):
        """Go into a select loop until all our jobs have completed or failed"""
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
        job_connections = self._get_request_connections(job_requests)

        def is_request_incomplete(current_request):
            return not current_request.complete

        # Poll until we get responses for all our functions
        def continue_while_jobs_incomplete(any_activity):
            return any(is_request_incomplete(current_request) and not current_request.connection_failed for current_request in job_requests)

        self.poll_connections_until_stopped(job_connections, continue_while_jobs_incomplete, timeout=timeout)

        # Mark any job still in the queued state to timeout
        for current_request in job_requests:
            job_incomplete = is_request_incomplete(current_request)
            current_request.timed_out = job_incomplete
            if not job_incomplete:
                self.request_to_rotating_connection_queue.pop(current_request, None)

        return job_requests

    def get_job_status(self, current_request, timeout=None):
        """Fetch the job status of a single request"""
        request_list = self.get_job_statuses([current_request], timeout=timeout)
        return gearman.util.unlist(request_list)

    def get_job_statuses(self, job_requests, timeout=None):
        """Fetch the job status of a multiple requests"""
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
        for current_request in job_requests:
            current_request.server_status['last_time_received'] = current_request.server_status.get('time_received')

            current_connection = current_request.job.connection
            current_command_handler = self.connection_to_handler_map[current_connection]

            current_command_handler.send_get_status_of_job(current_request)

        return self.wait_until_job_statuses_received(job_requests, timeout=timeout)

    def wait_until_job_statuses_received(self, job_requests, timeout=None):
        """Go into a select loop until we received statuses on all our requests"""
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
        job_connections = self._get_request_connections(job_requests)

        def is_status_not_updated(current_request):
            return bool(current_request.server_status.get('time_received') == current_request.server_status.get('last_time_received'))

        # Poll to make sure we send out our request for a status update
        def continue_while_status_not_updated(any_activity):
            return any(bool(is_status_not_updated(current_request) and not current_request.connection_failed) for current_request in job_requests)

        self.poll_connections_until_stopped(job_connections, continue_while_status_not_updated, timeout=timeout)

        for current_request in job_requests:
            current_request.server_status = current_request.server_status or {}
            current_request.timed_out = is_status_not_updated(current_request)

        return job_requests

    def _get_request_connections(self, job_requests):
        submitted_job_connections = set(current_request.job.connection for current_request in job_requests)
        submitted_job_connections.discard(None)

        return submitted_job_connections

    def _create_request_from_dictionary(self, job_info, background=False):
        """Takes a dictionary with fields  {'task': task, 'unique': unique, 'data': data, 'priority': priority, 'background': background}"""
        # Make sure we have a unique identifier for ALL our tasks
        job_unique = job_info.get('unique')
        if job_unique == '-':
            job_unique = job_info['data']
        elif not job_unique:
            job_unique = os.urandom(RANDOM_UNIQUE_BYTES).encode('hex')

        current_job = GearmanJob(connection=None, handle=None, task=job_info['task'], unique=job_unique, data=job_info['data'])
        current_request = GearmanJobRequest(current_job, initial_priority=job_info.get('priority', PRIORITY_NONE), background=background)
        return current_request

    def _choose_request_connection(self, current_request):
        """Return a live connection for the given hash"""
        # We'll keep track of the connections we're attempting to use so if we ever have to retry, we can use this history
        rotating_connections = self.request_to_rotating_connection_queue.get(current_request, None)
        if not rotating_connections:
            shuffled_connection_list = list(self.connection_list)
            random.shuffle(shuffled_connection_list)

            rotating_connections = collections.deque(shuffled_connection_list)
            self.request_to_rotating_connection_queue[current_request] = rotating_connections

        skipped_connections = 0
        chosen_connection = None
        for possible_conn in rotating_connections:
            chosen_connection = self.attempt_connect(possible_conn)
            if chosen_connection is not None:
                break

            skipped_connections += 1

        if chosen_connection is None:
            raise ServerUnavailable('Found no valid connections: %r' % self.connection_list)

        # Rotate our server list so we'll skip all our broken servers
        rotating_connections.rotate(-skipped_connections)
        return chosen_connection

    def handle_error(self, current_connection):
        """When we enter a connection error state, we should mark all requests as connection_failed

        This doesn't have much meaning for backgrounded requests
        """
        current_handler = self.connection_to_handler_map[current_connection]
        pending_requests, inflight_requests = current_handler.get_requests()

        for current_request in pending_requests:
            current_request.connection_failed = True

        for current_request in inflight_requests:
            current_request.connection_failed = True

        super(GearmanClient, self).handle_error(current_connection)
