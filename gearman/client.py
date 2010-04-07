#!/usr/bin/env python
# TODO: Implement retry behavior
# TODO: Test rotating servers

import collections
import time
import uuid
import random
import logging

import gearman.util
from gearman._client_base import GearmanClientBase
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
        super(GearmanClient, self).__init__(*args, **kwargs)

        # The authoritative copy of all reuqests that this client knows about
        # Ignores the fact if a request has been bound to a connection or not
        # Keyed on a tuple of (function_name, unique)
        self.request_to_rotating_connection_queue = collections.defaultdict(collections.deque)

        # 2 level dictionary of [conn][handle] pointing to a current request
        self.conn_handle_to_request_map = collections.defaultdict(dict)

        # When we first submit jobs, we don't have a handle assigned yet... these handles will be returned in the order of submission
        self.conn_to_pending_request_map = collections.defaultdict(collections.deque)

        # Setup our command handlers
        self.command_handlers = {
            GEARMAN_COMMAND_JOB_CREATED: self.recv_job_created,
            GEARMAN_COMMAND_WORK_COMPLETE: self.recv_work_complete,
            GEARMAN_COMMAND_WORK_FAIL: self.recv_work_fail,
            GEARMAN_COMMAND_WORK_EXCEPTION: self.recv_work_exception,
            GEARMAN_COMMAND_WORK_STATUS: self.recv_work_status,
            GEARMAN_COMMAND_WORK_DATA: self.recv_work_data,
            GEARMAN_COMMAND_WORK_WARNING: self.recv_work_warning,
            GEARMAN_COMMAND_ERROR: self.recv_error,
            GEARMAN_COMMAND_STATUS_RES: self.recv_status_res
        }

    def bind_connection_to_request(self, current_request):
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
        current_request.bind_connection(chosen_conn)

    ######################## BEGIN NEW CODE ######################## 
    def submit_job(self, function_name, data, unique=None, priority=NO_PRIORITY, background=FOREGROUND_JOB, timeout=None):
        job_info = dict(function_name=function_name, data=data, unique=unique)
        completed_job_list = self.submit_job_list([job_info], priority=priority, background=background, timeout=timeout)
        return completed_job_list[0]

    def submit_job_list(self, jobs_to_submit, priority=NO_PRIORITY, background=FOREGROUND_JOB, timeout=None):
        """Takes a list of jobs_to_submit with dicts of
        
        {'function_name': function_name, 'unique': unique, 'data': data}
        """
        submitted_job_requests = []
        for job_info in jobs_to_submit:
            current_request  = self._create_request_from_dictionary(job_info, priority=priority, background=background)
            self.bind_connection_to_request(current_request)

            self._submit_job_request(current_request)

            submitted_job_requests.append(current_request)

        return self._wait_for_jobs(submitted_job_requests, timeout=timeout)

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

    def _submit_job_request(self, current_request):
        conn = current_request.get_connection()
        pending_handle_queue = self.conn_to_pending_request_map[conn]
        pending_handle_queue.append(current_request)

        # Convert our job and 
        gearman_job = current_request.get_job()
        cmd_args = dict(func=gearman_job.func, data=gearman_job.data, unique=gearman_job.unique)

        cmd_type = self._submit_cmd_for_background_priority(current_request.is_background, current_request.priority)
        conn.send_command(cmd_type, cmd_args)

    def _submit_cmd_for_background_priority(self, background, priority):
        cmd_type_lookup = {
            (BACKGROUND_JOB, NO_PRIORITY): GEARMAN_COMMAND_SUBMIT_JOB_BG,
            (BACKGROUND_JOB, LOW_PRIORITY): GEARMAN_COMMAND_SUBMIT_JOB_LOW_BG,
            (BACKGROUND_JOB, HIGH_PRIORITY): GEARMAN_COMMAND_SUBMIT_JOB_HIGH_BG,
            (FOREGROUND_JOB, NO_PRIORITY): GEARMAN_COMMAND_SUBMIT_JOB,
            (FOREGROUND_JOB, LOW_PRIORITY): GEARMAN_COMMAND_SUBMIT_JOB_LOW,
            (FOREGROUND_JOB, HIGH_PRIORITY): GEARMAN_COMMAND_SUBMIT_JOB_HIGH            
        }
        lookup_tuple = (background, priority)
        cmd_type = cmd_type_lookup[lookup_tuple]
        return cmd_type

    def _wait_for_jobs(self, submitted_job_requests, timeout=None):
        """Keep polling on our connection until our job requests are complete"""
        submitted_job_connections = set(current_request.get_connection() for current_request in submitted_job_requests)
        submitted_job_connections.discard(None)

        # Stop polling when all our jobs are complete
        def callback_on_possible_job_completion(self, any_activity):
            return not all(job_request.is_complete() for job_request in submitted_job_requests)

        self.poll_connections_until_stopped(submitted_job_connections, callback_on_possible_job_completion, timeout=timeout)

        # Mark any job still in the queued state to timeout
        for current_request in submitted_job_requests:
            if not current_request.is_complete():
                current_request.state = GEARMAN_JOB_STATE_TIMEOUT

        return submitted_job_requests

    def get_status(self, current_request, timeout=None):
        "Blocking get_status request"
        current_connection = current_request.get_connection()
        current_handle = current_request.get_handle()

        current_connection.send_command(GEARMAN_COMMAND_GET_STATUS, dict(handle=current_handle))

        # Set an instance variable tracking saying we're waiting for a status response
        self._returned_status_response = {}

        # Stop polling when we're no longer waiting on our status response
        def callback_on_possible_status_response(self, any_activity):
            return not bool(self._returned_status_response)

        self.poll_connections_until_stopped([current_connection], callback_on_possible_status_response, timeout=timeout)
        
        received_response = self._returned_status_response
        del self._returned_status_response

        return received_response

    # Gearman worker callbacks when we need to retry a job request
    def on_job_request_retry(self, current_request):
        # conn_handle = current_request.connection_handle()
        # del self.conn_handle_to_key_map[conn_handle]
        # self._submit_job_request(current_request)
        raise NotImplementedError

        # This request is no longer tied to this connection
        del self.conn_handle_to_request_map[conn_handle]

    def on_job_request_failed(self, current_request):
        current_request.state = GEARMAN_JOB_STATE_FAILED

    ##########################################################################
    ### Gearman worker callbacks when we receive a command from the server ###
    ##########################################################################
    def recv_job_created(self, conn, cmd_type, cmd_args):
        handle = cmd_args['handle']

        request_queue = self.conn_to_pending_request_map[conn]

        current_request = request_queue.popleft()
        current_request.bind_handle(handle)
        current_request.state = GEARMAN_JOB_STATE_QUEUED
        
        # Once we know that a job's been created, 
        self.conn_handle_to_request_map[conn][handle] = current_request
        return True

    def recv_work_data(self, conn, cmd_type, cmd_args):
        current_request = self.conn_handle_to_request_map[conn][cmd_args['handle']]
        current_request.data_updates.append(cmd_args['data'])

        return True

    def recv_work_warning(self, conn, cmd_type, cmd_args):
        current_request = self.conn_handle_to_request_map[conn][cmd_args['handle']]
        current_request.warning_updates.append(cmd_args['data'])
        
        return True

    def recv_work_status(self, conn, cmd_type, cmd_args):
        current_request = self.conn_handle_to_request_map[conn][cmd_args['handle']]

        status_tuple = (float(cmd_args['numerator']), float(cmd_args['denominator']))
        current_request.status_updates.append(status_tuple)
        return True

    def recv_work_complete(self, conn, cmd_type, cmd_args):
        current_request = self.conn_handle_to_request_map[conn][cmd_args['handle']]
        current_request.result = cmd_args['data']
        current_request.state = GEARMAN_JOB_STATE_COMPLETE

        current_request.bind_connection(None)

        return True

    def recv_work_fail(self, conn, cmd_type, cmd_args):
        current_request = self.conn_handle_to_request_map[conn][cmd_args['handle']]
        if current_request.retries_attempted < current_request.retries_max:
            self.on_job_request_retry(current_request)
        else:
            self.on_job_request_failed(current_request)
        
        return True

    def recv_work_exception(self, conn, cmd_type, cmd_args):
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        current_request = self.conn_handle_to_request_map[conn][cmd_args['handle']]
        current_request.exception = cmd_args['data']

        return True

    def recv_error(self, conn, cmd_type, cmd_args):
        gearman_logger.error("Error from server: %s: %s" % (cmd_args['err_code'], cmd_args['err_text']))
        conn.close()

        return False

    def recv_status_res(self, conn, cmd_type, cmd_args):
        transformed_status = {
            'handle': cmd_args['handle'],
            'known': bool(cmd_args['known'] == '1'),
            'running': bool(cmd_args['running'] == '1'),
            'numerator': float(cmd_args['numerator']),
            'denominator': float(cmd_args['denominator'])
        }

        # Return a status request
        self._returned_status_response = transformed_status

        return True
