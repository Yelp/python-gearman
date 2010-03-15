#!/usr/bin/env python
import collections
import errno
import select
import time
import uuid
import random
import logging

import gearman.util
from gearman.compat import *
from gearman.connection import GearmanConnection, GearmanConnectionError
from gearman.task import Task, Taskset
from gearman.protocol import *
from gearman.job import *

FOREGROUND_JOB = False
BACKGROUND_JOB = True

NO_PRIORITY = None
LOW_PRIORITY = "low"
HIGH_PRIORITY = "high"

gearman_logger = logging.getLogger("gearman._client")

class GearmanBaseClient(object):
    class ServerUnavailable(Exception):
        pass
    class CommandError(Exception):
        pass
    class InvalidResponse(Exception):
        pass

    client_type = None

    def __init__(self, host_list, blocking_timeout=0.0):
        self.command_handlers = {}
        self.connection_list = []

        for hostport_tuple in host_list:
            gearman_host, gearman_port = self.disambiguate_server_parameter(hostport_tuple)
            client_connection = GearmanConnection(gearman_host, gearman_port, blocking_timeout=blocking_timeout)
            self.connection_list.append(client_connection)

    def disambiguate_server_parameter(self, hostport_tuple):
        if type(hostport_tuple) is tuple:
            gearman_host, gearman_port = hostport_tuple
        else:
            gearman_host = hostport_tuple
            gearman_port = DEFAULT_GEARMAN_PORT

        return gearman_host, gearman_port

    def poll_connections(self, connections, timeout=None):
        """Returns True if we did any activity, False if not"""
        all_conns = set(connections)
        dead_conns = set()
        select_conns = all_conns

        rd_list = []
        wr_list = []
        ex_list = []

        successful_select = False
        while not successful_select and select_conns:
            select_conns = all_conns - dead_conns
            rx_conns = [c for c in select_conns if c.readable()]
            tx_conns = [c for c in select_conns if c.writable()]
    
            try:
                rd_list, wr_list, ex_list = gearman.util.select(rx_conns, tx_conns, select_conns, timeout=timeout)
                successful_select = True
            except:
                # Fish for the bad connections
                for conn_to_test in select_conns:
                    try:
                        _, _, _ = gearman.util.select([conn_to_test], [], [], timeout=0)
                    except:
                        dead_conns.add(conn_to_test)

        for conn in rd_list:
            self.handle_read(conn)

        for conn in wr_list:
            self.handle_write(conn)

        for conn in ex_list:
            self.handle_error(conn)

        for conn in dead_conns:
            self.handle_error(conn)

        return any([rd_list, wr_list, ex_list])

    def handle_read(self, conn):
        """Read behavior MUST be specified by the inheriting class

        It is ambiguous whether or not we want to do blocking reads"""
        raise NotImplementedError

    def handle_incoming_command(self, conn, cmd_tuple):
        completed_work = None
        if cmd_tuple is None:
            return completed_work

        cmd_type, cmd_args = cmd_tuple
        if cmd_type not in self.command_handlers:
            raise KeyError("Received an unexpected cmd_type: %r not in %r" % (GEARMAN_COMMAND_TO_NAME.get(cmd_type, cmd_type), self.command_handlers.keys()))

        cmd_callback = self.command_handlers[cmd_type]
        completed_work = cmd_callback(conn, cmd_type, cmd_args)
        return completed_work

    def handle_write(self, conn):
        while conn.writable():
            conn.send_data_from_buffer()

    def handle_error(self, conn):
        gearman_logger.error("Exception on connection %s" % c)
        conn.close()

class GearmanClient(GearmanBaseClient):
    client_type = "client"

    def __init__(self, *args, **kwargs):
        # By default we should have non-blocking sockets for a GearmanClient
        kwargs.setdefault('blocking_timeout', 0.0)
        super(GearmanClient, self).__init__(*args, **kwargs)

        # Support lookups of keys to requests
        self.key_to_request_map = {}
        self.connection_handle_to_key_map = {}
        
        self.command_handlers = {
            GEARMAN_COMMAND_JOB_CREATED: self.recv_job_created,
            GEARMAN_COMMAND_WORK_COMPLETE: self.recv_work_complete,
            GEARMAN_COMMAND_WORK_FAIL: self.recv_work_fail,
            GEARMAN_COMMAND_WORK_EXCEPTION: self.recv_work_exception,
            GEARMAN_COMMAND_WORK_STATUS: self.recv_work_status,
            GEARMAN_COMMAND_WORK_DATA: self.recv_work_data,
            GEARMAN_COMMAND_WORK_WARNING: self.recv_work_warning,
            GEARMAN_COMMAND_ERROR: self.recv_error
        }

    def choose_server_for_hash(self, hsh):
        """Return a live connection for the given hash"""
        # TODO: instead of cycling through, should we shuffle the list if the first connection fails or is dead?
        first_idx = hsh % len(self.connection_list)
        all_dead = all(not conn.is_connected() for conn in self.connection_list)
        for idx in range(first_idx, len(self.connection_list)) + range(0, first_idx):
            conn = self.connection_list[idx]

            # if all of the connections are dead we should try reconnecting
            if not conn.is_connected() and not all_dead:
                continue

            try:
                conn.connect() # Make sure the connection is up (noop if already connected)
            except GearmanConnectionError:
                pass
            else:
                return conn

        raise self.ServerUnavailable("Unable to Locate Server")

    ######################## BEGIN NEW CODE ######################## 
    def handle_read(self, conn):
        """For our worker, we'll want to do blocking calls on processing out commands"""
        # Do a single NON blocking read
        cmd_list = conn.recv_command_list()
        for cmd_tuple in cmd_list:
            completed_work = self.handle_incoming_command(conn, cmd_tuple)
            if completed_work is False:
                break

    def submit_job(self, function_name, data, unique=None, background=FOREGROUND_JOB, priority=NO_PRIORITY, timeout=None):
        job_info = dict(function_name=function_name, data=data, unique=unique)
        completed_job_list = self.submit_job_list([job_info], background=background, priority=priority, timeout=timeout)
        return completed_job_list[0]

    def submit_job_list(self, jobs_to_submit, background=FOREGROUND_JOB, priority=NO_PRIORITY, timeout=None):
        """Takes a list of jobs_to_submit with dicts of
        
        {'function_name': function_name, 'unique': unique, 'data': data}
        """
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

        submitted_job_requests = []
        for job_info in jobs_to_submit:

            # Make sure we have a unique identifier for ALL our tasks
            job_unique = job_info.get('unique')
            job_unique = job_unique or uuid.uuid4().hex

            current_job = GearmanJob(conn=None, handle=None, function_name=job_info['function_name'], unique=job_unique, data=job_info['data'])
            current_request = GearmanJobRequest(current_job, submit_cmd=cmd_type, initial_priority=priority, is_background=background)

            submitted_job_requests.append(current_request)

            request_key = current_request.unique_key()
            self.key_to_request_map[request_key] = current_request
            self._submit_job_request(current_request)

        return self._wait_for_job_completions(submitted_job_requests, timeout=timeout)

    def _submit_job_request(self, given_job_request):
        conn = self.choose_server_for_hash(hash(given_job_request))
        given_job_request.bind_connection(conn)

        gearman_job = given_job_request.get_job()
        cmd_args = dict(func=gearman_job.func, data=gearman_job.data, unique=gearman_job.unique)

        # Do non-blocking sends of this command
        conn.send_command(given_job_request.submit_cmd, cmd_args)
        conn.waiting_for_handles.append(given_job_request.unique_key())

    def _wait_for_job_completions(self, submitted_job_requests, timeout=None):
        submitted_job_connections = set(current_job.get_connection() for current_job in submitted_job_requests)
        submitted_job_connections -= set([None])
        
        continue_working = True
        while continue_working:
            # time_remaining = time.time() - time_started

            any_activity = self.poll_connections(submitted_job_connections, timeout=None)

            jobs_complete = all(job_request.is_complete() for job_request in submitted_job_requests)
            # have_time_remaining = bool(time_remaining > 0)
            have_time_remaining = True
            continue_working = all([not jobs_complete, have_time_remaining, any_activity])

        return submitted_job_requests
    # 
    # def get_status(self, handle):
    #     current_request = self.fetch_job_request(conn, cmd_args['handle'])
    #     current_connection = current_request.get_connection()
    # 
    #     current_connection.send_command(GEARMAN_COMMAND_GET_STATUS, dict(handle=shandle))
    #     cmd_tuple = current_connection.recv_command()
    #     if cmd_tuple is None:
    #         return None
    # 
    #     cmd_type, cmd_args = cmd_tuple
    #     return cmd_args

    def fetch_job_request(self, conn, handle):
        connection_handle_key = (conn.hostspec, handle)
        request_key = self.connection_handle_to_key_map[connection_handle_key]
        return self.key_to_request_map[request_key]

    # Gearman worker callbacks when we receive a command from the server
    def recv_job_created(self, conn, cmd_type, cmd_args):
        handle = cmd_args['handle']
        request_key = conn.waiting_for_handles.popleft()

        current_request = self.key_to_request_map[request_key]
        current_request.bind_handle(handle)
        current_request.state = GEARMAN_JOB_STATE_QUEUED
        
        # Now we have a connection tied to this guy
        conn_handle = current_request.connection_handle()
        self.connection_handle_to_key_map[conn_handle] = request_key
        return True

    def recv_work_data(self, conn, cmd_type, cmd_args):
        current_request = self.fetch_job_request(conn, cmd_args['handle'])
        current_request.data_updates.append(cmd_args['data'])

        return True

    def recv_work_warning(self, conn, cmd_type, cmd_args):
        current_request = self.fetch_job_request(conn, cmd_args['handle'])
        current_request.warning_updates.append(cmd_args['data'])
        
        return True

    def recv_work_status(self, conn, cmd_type, cmd_args):
        current_request = self.fetch_job_request(conn, cmd_args['handle'])

        status_tuple = (cmd_args['numerator'], cmd_args['denominator'])
        current_request.status_updates.append(status_tuple)
        return True

    def recv_work_complete(self, conn, cmd_type, cmd_args):
        current_request = self.fetch_job_request(conn, cmd_args['handle'])
        current_request.result = cmd_args['data']
        current_request.state = GEARMAN_JOB_STATE_COMPLETE

        conn_handle = current_request.connection_handle()
        del self.connection_handle_to_key_map[conn_handle]

        current_request.bind_connection(None)

        return True

    def recv_work_fail(self, conn, cmd_type, cmd_args):
        current_request = self.fetch_job_request(conn, cmd_args['handle'])
        if current_request.retries_attempted < current_request.retries_max:
            self.on_job_request_retry(current_request)
        else:
            self.on_job_request_failed(current_request)
        
        return True

    def recv_work_exception(self, conn, cmd_type, cmd_args):
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        current_request = self.fetch_job_request(conn, cmd_args['handle'])
        current_request.exception = cmd_args['data']

        return True

    def recv_error(self, conn, cmd_type, cmd_args):
        gearman_logger.error("Error from server: %s: %s" % (cmd_args['err_code'], cmd_args['err_text']))
        conn.close()

        return False

    # Gearman worker callbacks when we receive a command from the server
    def on_job_request_retry(self, current_request):
        # conn_handle = current_request.connection_handle()
        # del self.connection_handle_to_key_map[conn_handle]
        # self._submit_job_request(current_request)
        raise NotImplementedError

        del self.connection_handle_to_key_map[conn_handle]
        return True

    def on_job_request_failed(self, current_request):
        current_request.state = GEARMAN_JOB_STATE_FAILED
        return True
