import collections
import logging
import os
import random
import time

import gearman.util

from gearman.connection_manager import GearmanConnectionManager
from gearman.client_handler import GearmanClientCommandHandler
from gearman.constants import PRIORITY_NONE, PRIORITY_LOW, PRIORITY_HIGH, JOB_UNKNOWN, JOB_PENDING
from gearman.errors import ServerUnavailable, ConnectionError, InvalidClientState, ExceededConnectionAttempts
from gearman.job import GearmanJob, GearmanJobRequest

gearman_logger = logging.getLogger(__name__)

RANDOM_UNIQUE_BYTES = 16

class GearmanClient(GearmanConnectionManager):
    """
    GearmanClient :: Interface to submit jobs to a Gearman server

    Submits a single/multiple jobs to a gearman server
    """
    command_handler_class = GearmanClientCommandHandler

    def __init__(self, host_list=None, random_unique_bytes=RANDOM_UNIQUE_BYTES):
        super(GearmanClient, self).__init__(host_list=host_list)

        self.random_unique_bytes = random_unique_bytes

        # The authoritative copy of all requests that this client knows about
        # Ignores the fact if a request has been bound to a connection or not
        self.request_to_rotating_connection_queue = collections.defaultdict(collections.deque)

    def submit_job(self, task, data, unique=None, priority=PRIORITY_NONE, background=False, wait_until_complete=True, max_retries=0, timeout=None):
        """Submit a single job to any gearman server"""
        job_info = dict(task=task, data=data, unique=unique, priority=priority)
        completed_job_list = self.submit_multiple_jobs([job_info], background=background, wait_until_complete=wait_until_complete, max_retries=max_retries, timeout=timeout)
        return gearman.util.unlist(completed_job_list)

    def submit_multiple_jobs(self, jobs_to_submit, background=False, wait_until_complete=True, max_retries=0, timeout=None):
        """Takes a list of jobs_to_submit with dicts of

        {'task': task, 'data': data, 'unique': unique, 'priority': priority}
        """
        assert type(jobs_to_submit) in (list, tuple, set), "Expected multiple jobs, received 1?"

        # Convert all job dicts to job request objects
        max_attempts = max_retries + 1
        requests_to_submit = [self._create_request_from_dictionary(job_info, background=background, max_attempts=max_attempts) for job_info in jobs_to_submit]

        return self.submit_multiple_requests(requests_to_submit, wait_until_complete=wait_until_complete, timeout=timeout)

    def submit_multiple_requests(self, job_requests, wait_until_complete=True, timeout=None):
        """Take GearmanJobRequests, assign them connections, and request that they be done.

        * Blocks until our jobs are accepted (should be fast) OR times out
        * Optionally blocks until jobs are all complete

        You MUST check the status of your requests after calling this function as "timed_out" or "state == JOB_UNKNOWN" maybe True
        """
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"
        stopwatch = gearman.util.Stopwatch(timeout)

        # We should always wait until our job is accepted, this should be fast
        time_remaining = stopwatch.get_time_remaining()
        processed_requests = self.wait_until_jobs_accepted(job_requests, timeout=time_remaining)

        # Optionally, we'll allow a user to wait until all jobs are complete with the same timeout
        time_remaining = stopwatch.get_time_remaining()
        if wait_until_complete and bool(time_remaining != 0.0):
            processed_requests = self.wait_until_jobs_completed(processed_requests, timeout=time_remaining)

        return processed_requests

    def wait_until_jobs_accepted(self, job_requests, timeout=None):
        """Go into a select loop until all our jobs have moved to STATE_PENDING"""
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"

        def is_request_pending(current_request):
            return bool(current_request.state == JOB_PENDING)

        # Poll until we know we've gotten acknowledgement that our job's been accepted
        # If our connection fails while we're waiting for it to be accepted, automatically retry right here
        def continue_while_jobs_pending(any_activity):
            for current_request in job_requests:
                if current_request.state == JOB_UNKNOWN:
                    self._send_job_request(current_request)

            return any(is_request_pending(current_request) for current_request in job_requests)

        self.poll_connections_until_stopped(self.connection_list, continue_while_jobs_pending, timeout=timeout)

        # Mark any job still in the queued state to timeout
        for current_request in job_requests:
            current_request.timed_out = is_request_pending(current_request)

        return job_requests

    def wait_until_jobs_completed(self, job_requests, timeout=None):
        """Go into a select loop until all our jobs have completed or failed"""
        assert type(job_requests) in (list, tuple, set), "Expected multiple job requests, received 1?"

        def is_request_incomplete(current_request):
            return not current_request.complete

        # Poll until we get responses for all our functions
        # Do NOT attempt to auto-retry connection failures as we have no idea how for a worker got
        def continue_while_jobs_incomplete(any_activity):
            for current_request in job_requests:
                if is_request_incomplete(current_request) and current_request.state != JOB_UNKNOWN:
                    return True

            return False

        self.poll_connections_until_stopped(self.connection_list, continue_while_jobs_incomplete, timeout=timeout)

        # Mark any job still in the queued state to timeout
        for current_request in job_requests:
            current_request.timed_out = is_request_incomplete(current_request)

            if not current_request.timed_out:
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
        def is_status_not_updated(current_request):
            return bool(current_request.server_status.get('time_received') == current_request.server_status.get('last_time_received'))

        # Poll to make sure we send out our request for a status update
        def continue_while_status_not_updated(any_activity):
            for current_request in job_requests:
                if is_status_not_updated(current_request) and current_request.state != JOB_UNKNOWN:
                    return True

            return False

        self.poll_connections_until_stopped(self.connection_list, continue_while_status_not_updated, timeout=timeout)

        for current_request in job_requests:
            current_request.server_status = current_request.server_status or {}
            current_request.timed_out = is_status_not_updated(current_request)

        return job_requests

    def _create_request_from_dictionary(self, job_info, background=False, max_attempts=1):
        """Takes a dictionary with fields  {'task': task, 'unique': unique, 'data': data, 'priority': priority, 'background': background}"""
        # Make sure we have a unique identifier for ALL our tasks
        job_unique = job_info.get('unique')
        if job_unique == '-':
            job_unique = job_info['data']
        elif not job_unique:
            job_unique = os.urandom(self.random_unique_bytes).encode('hex')

        current_job = self.job_class(connection=None, handle=None, task=job_info['task'], unique=job_unique, data=job_info['data'])
        current_request = self.job_request_class(current_job, initial_priority=job_info.get('priority', PRIORITY_NONE), background=background, max_attempts=max_attempts)
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

    def _send_job_request(self, current_request):
        """Attempt to send out a job request"""
        if current_request.connection_attempts >= current_request.max_connection_attempts:
            raise ExceededConnectionAttempts("Exceeded %d connection attempt(s) :: %r" % (current_request.max_connection_attempts, current_request))

        chosen_connection = self._choose_request_connection(current_request)

        current_request.job.connection = chosen_connection
        current_request.connection_attempts += 1
        current_request.timed_out = False

        current_command_handler = self.connection_to_handler_map[chosen_connection]
        current_command_handler.send_job_request(current_request)
        return current_request
