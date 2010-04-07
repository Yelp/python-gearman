# TODO: Write client_get_status
# TODO: Catch client failures and purge all foreground jobs from the queue

import logging
import random
import time
import socket
import collections

import gearman.util
from gearman.protocol import *
from gearman.constants import DEFAULT_GEARMAN_PORT
from gearman._client_base import GearmanClientBase
from gearman.connection import GearmanConnection
from gearman.errors import ConnectionError
from gearman.job import GearmanJob, GearmanJobRequest

FOREGROUND_JOB = False
BACKGROUND_JOB = True

NO_PRIORITY = None
LOW_PRIORITY = "low"
HIGH_PRIORITY = "high"

gearman_logger = logging.getLogger("gearman.server")

class GearmanJobManager(object):
    """NOTE:  This server was built for TEST-only purposes.

    This should NOT be used in production as it hasn't been thoroughly tested
    MANY of the server behaviors here may NOT match the reference gearmand implementation written in C
    """
    def __init__(self):
        self.task_to_request_queue_map = collections.defaultdict(collections.deque)

        # We need to keep track of all workers in case we need to wake someone up
        self.task_to_worker_map = collections.defaultdict(set)
        self.worker_to_task_map = collections.defaultdict(set)
        self.sleeping_workers = set()

        self.handle_to_request_map = {}

        self.task_to_handles_in_progress = collections.defaultdict(set)

        # Keep track of which clients and workers know about this
        self.handle_to_client_map = {}
        self.handle_to_worker_map = {}
        self.handle_counter = 0

    def get_server_status(self):
        status_list = []
        for function_name, workers in self.task_to_worker_map.iteritems():
            queued_jobs = len(self.task_to_request_queue_map[function_name])
            working_jobs = len(self.task_to_handles_in_progress[function_name])
            
            function_status = dict(func=function_name)
            function_status['num_jobs'] = queued_jobs + working_jobs
            function_status['num_working'] = working_jobs
            function_status['num_workers'] = len(workers)
            status_list.append(function_status)

        return status_list

    def get_server_workers(self):
        worker_list = []
        for worker_conn, function_set in self.worker_to_task_map.iteritems():
            worker_status = {}
            worker_status['fd'] = worker_conn.fileno()
            gearman_host, gearman_port = worker_conn.get_address()
            worker_status['host'] = gearman_host
            worker_status['client_id'] = None
            worker_status['abilities'] = function_set
            worker_list.append(worker_status)

        return worker_list

    def generate_handle(self):
        self.handle_counter += 1
        return "%s-%s" % (id(self), self.handle_counter)

    def check_queues(self):
        command_list = []

        # See if we need to wake anyone up
        for task_name, request_queue in self.task_to_request_queue_map.iteritems():
            if not request_queue:
                continue

            all_workers = self.task_to_worker_map[task_name]
            possibly_sleeping_workers = all_workers & self.sleeping_workers

            # Wake up workers who might be able to take this job
            for worker_conn in possibly_sleeping_workers:
                command_list.append((worker_conn, GEARMAN_COMMAND_NOOP, dict()))
                self.sleeping_workers.discard(worker_conn)

        return command_list

    def any_echo_req(self, gearman_conn, text):
        return [(gearman_conn, GEARMAN_COMMAND_ECHO_RES, dict(text=text))]

    def client_submit_job(self, gearman_job, initial_priority=NO_PRIORITY, is_background=FOREGROUND_JOB):
        """Queue job does NOT check for collisions on unique"""
        handle = self._add_job(gearman_job, priority=initial_priority, background=is_background)
        return [(gearman_job.conn, GEARMAN_COMMAND_JOB_CREATED, dict(handle=handle))]

    def worker_can_do(self, worker_conn, function_name):
        self.task_to_worker_map[function_name].add(worker_conn)
        self.worker_to_task_map[worker_conn].add(function_name)
        return []

    def worker_cant_do(self, worker_conn, function_name):
        self.task_to_worker_map[function_name].discard(worker_conn)
        self.worker_to_task_map[worker_conn].discard(function_name)
        return []

    def worker_reset_abilities(self, worker_conn):
        all_functions = self.worker_to_task_map[worker_conn]
        for function_name in all_functions:
            self.worker_cant_do(worker_conn, function_name)

        return []

    def worker_grab_job(self, worker_conn, is_unique):
        current_request = self._assign_job(worker_conn, is_unique=is_unique)
        cmd_type = GEARMAN_COMMAND_NO_JOB
        cmd_args = {}
        if current_request:
            current_job = current_request.get_job()
            cmd_type = GEARMAN_COMMAND_JOB_ASSIGN
            cmd_args = dict(handle=current_job.handle, func=current_job.func, data=current_job.data)

            if is_unique:
                cmd_type = GEARMAN_COMMAND_JOB_ASSIGN_UNIQ
                cmd_args['unique'] = current_job.unique

            # Really this is making an assumption that we were able to connect to a worker and say that we sent them this job...
            self.handle_to_worker_map[current_job.handle] = worker_conn
            self.task_to_handles_in_progress[current_job.func].add(current_job.handle)

        return [(worker_conn, cmd_type, cmd_args)]

    def worker_pre_sleep(self, worker_conn):
        self.sleeping_workers.add(worker_conn)
        return []

    def worker_work_complete(self, worker_conn, cmd_args):
        cmds_to_forward = self._forward_command_to_client(GEARMAN_COMMAND_WORK_COMPLETE, cmd_args)

        self._remove_job_by_handle(cmd_args['handle'])
        return cmds_to_forward

    def worker_work_fail(self, worker_conn, cmd_args):
        cmds_to_forward = self._forward_command_to_client(GEARMAN_COMMAND_WORK_FAIL, cmd_args)

        self._remove_job_by_handle(cmd_args['handle'])
        return cmds_to_forward

    def worker_work_data(self, worker_conn, cmd_args):
        cmds_to_forward = self._forward_command_to_client(GEARMAN_COMMAND_WORK_DATA, cmd_args)
        return cmds_to_forward

    def worker_work_warning(self, worker_conn, cmd_args):
        cmds_to_forward = self._forward_command_to_client(GEARMAN_COMMAND_WORK_WARNING, cmd_args)
        return cmds_to_forward

    def worker_work_status(self, worker_conn, cmd_args):
        cmds_to_forward = self._forward_command_to_client(GEARMAN_COMMAND_WORK_STATUS, cmd_args)
        return cmds_to_forward

    def unregister_connection(self, any_conn):
        # TODO: Gracefully handle a dead client connection, all this code is for dead worker connections

        # We don't know if we're dealing with a worker connection so lets clean this up as if it were
        possibled_tasks = self.worker_to_task_map[any_conn]
        for task_name in possibled_tasks:
            self.task_to_worker_map[task_name].discard(any_conn)
    
        self.worker_to_task_map.pop(any_conn, None)
        self.sleeping_workers.discard(any_conn)

        # Try to find all failed job handles associated with this worker
        failed_job_handles = set()
        for job_handle, worker_conn in self.handle_to_worker_map.iteritems():
            # This is a worker that died... we better notify the client
            if any_conn is worker_conn:
                failed_job_handles.add(job_handle)

        # Clean up each handle
        output_commands = []
        for job_handle in failed_job_handles:
            current_request = self.handle_to_request_map.get(job_handle)
            if not current_request:
                continue

            self._unregister_request_for_client(current_request)
            self._unregister_request_for_worker(current_request)

        return output_commands

    def _unregister_request_for_client(self, current_request):
        current_job = current_request.get_job()
        job_handle = current_job.handle
        function_name = current_job.func

        if not current_request.background:
            task_queue = self.task_to_request_queue_map[function_name]
            task_queue.remove(current_request)

        # At the very last moment, update our client map and remove this handle from the queue
        self.handle_to_client_map.pop(job_handle, None)

    def _unregister_request_for_worker(self, current_request):
        current_job = current_request.get_job()
        job_handle = current_job.handle
        function_name = current_job.func

        # This job is no longer in progress nor is it tied to a particular worker
        self.task_to_handles_in_progress[function_name].discard(job_handle)

        # If this is a background job, see if we should try this job
        if current_request.is_background:
            if current_request.retries_attempted >= current_request.retries_max:
                gearman_logger.error("Discarding job %r due to too many job failures, max %d", current_job, current_request.retries_max)
            else:
                current_request.retries_attempted += 1

                # Requeue this handle to the front of the queue
                self.task_to_request_queue_map[function_name].appendleft(current_request)

        # If this is a foreground job, we need to forward the work failure along
        elif current_job and current_job.conn:
            output_commands.append((current_job.conn, GEARMAN_COMMAND_WORK_FAIL, dict(handle=job_handle)))

        # At the very last moment, update our worker map and remove this handle from the queue
        self.handle_to_worker_map.pop(job_handle, None)

    def _add_job(self, gearman_job, priority=None, background=None):
        job_handle = self.generate_handle()
        
        current_request = GearmanJobRequest(gearman_job, initial_priority=priority, is_background=background)
        current_request.bind_handle(job_handle)

        self.handle_to_request_map[job_handle] = current_request
        if background:
            self.handle_to_client_map[job_handle] = gearman_job.conn

        task_name = gearman_job.func
        task_queue = self.task_to_request_queue_map[task_name]
        task_queue.append(current_request)
        return job_handle

    def _assign_job(self, worker_conn, is_unique):
        all_functions = list(self.worker_to_task_map[worker_conn])

        # Randomize what task we might get assigned
        random.shuffle(all_functions)
        
        current_request = None
        for random_function in all_functions:
            working_queue = self.task_to_request_queue_map[random_function]
            if not working_queue:
                continue

            current_request = working_queue.popleft()
            break

        return current_request
        
    def _remove_job_by_handle(self, job_handle):
        current_request = self.handle_to_request_map[job_handle]
        gearman_job = current_request.get_job()

        # This job may not have a client if it were backgrounded
        self.handle_to_client_map.pop(job_handle, None)
        self.handle_to_worker_map[job_handle]

        self.task_to_handles_in_progress[gearman_job.func].discard(job_handle)

        del self.handle_to_request_map[job_handle]
        return None

    def _forward_command_to_client(self, cmd_type, cmd_args):
        handle = cmd_args['handle']
        current_request = self.handle_to_request_map[handle]
        
        current_job = current_request.get_job()
        client_conn = current_job.conn
        assert bool(client_conn) and client_conn.is_connected(), "Could not find a valid connection for this job: %r" % handle

        # Forward along these packets to the client connection
        if not current_request.is_background:
            return [(client_conn, cmd_type, cmd_args)]
        else:
            return []

class GearmanServer(GearmanClientBase):
    client_type = "server"
    
    def __init__(self, hostport_tuple):
        super(GearmanServer, self).__init__([])
        self.setup_command_handlers()
        self.manager = GearmanJobManager()

        gearman_host, gearman_port = gearman.util.disambiguate_server_parameter(hostport_tuple)

        server_connection = GearmanConnection(gearman_host, gearman_port, blocking_timeout=0.0)
        server_connection.listen(5)
        self.server_connection = server_connection

        self.connection_list.append(server_connection)

    def setup_command_handlers(self):
        self.command_handlers = {
            # Gearman commands 10-19
            GEARMAN_COMMAND_CAN_DO: self.recv_can_do,
            GEARMAN_COMMAND_CANT_DO: self.recv_cant_do,
            GEARMAN_COMMAND_RESET_ABILITIES: self.recv_reset_abilities,
            GEARMAN_COMMAND_PRE_SLEEP: self.recv_pre_sleep,
            GEARMAN_COMMAND_SUBMIT_JOB: self.recv_submit_job,
            GEARMAN_COMMAND_GRAB_JOB: self.recv_grab_job,

            # Gearman commands 10-19
            GEARMAN_COMMAND_WORK_STATUS: self.recv_work_status,
            GEARMAN_COMMAND_WORK_COMPLETE: self.recv_work_complete,
            GEARMAN_COMMAND_WORK_FAIL: self.recv_work_fail,
            GEARMAN_COMMAND_GET_STATUS: self.recv_get_status,
            GEARMAN_COMMAND_ECHO_REQ: self.recv_echo_req,
            GEARMAN_COMMAND_SUBMIT_JOB_BG: self.recv_submit_job_bg,

            # Gearman commands 20-29
            GEARMAN_COMMAND_SUBMIT_JOB_HIGH: self.recv_submit_job_high,
            GEARMAN_COMMAND_SET_CLIENT_ID: self.recv_set_client_id,
            GEARMAN_COMMAND_CAN_DO_TIMEOUT: self.recv_can_do_timeout,
            GEARMAN_COMMAND_ALL_YOURS: self.recv_all_yours,
            GEARMAN_COMMAND_WORK_EXCEPTION: self.recv_work_exception,
            GEARMAN_COMMAND_OPTION_REQ: self.recv_option_req,
            GEARMAN_COMMAND_WORK_DATA: self.recv_work_data,
            GEARMAN_COMMAND_WORK_WARNING: self.recv_work_warning,

            # Gearman commands 30-39
            GEARMAN_COMMAND_GRAB_JOB_UNIQ: self.recv_grab_job_uniq,
            GEARMAN_COMMAND_SUBMIT_JOB_HIGH_BG: self.recv_submit_job_high_bg,
            GEARMAN_COMMAND_SUBMIT_JOB_LOW: self.recv_submit_job_low,
            GEARMAN_COMMAND_SUBMIT_JOB_LOW_BG: self.recv_submit_job_low_bg,

            GEARMAN_SERVER_COMMAND_STATUS: self.recv_server_status,
            GEARMAN_SERVER_COMMAND_VERSION: self.recv_server_version,
            GEARMAN_SERVER_COMMAND_WORKERS: self.recv_server_workers,
            GEARMAN_SERVER_COMMAND_MAXQUEUE: self.recv_server_maxqueue,
            GEARMAN_SERVER_COMMAND_SHUTDOWN: self.recv_server_shutdown,

            None: self.recv_invalid_binary_command
        }

    def handle_read(self, conn):
        if conn is self.server_connection:
            new_client_conn = conn.accept()
            self.connection_list.append(new_client_conn)

            gearman_logger.debug("Accepted connection from %r", new_client_conn.get_address())
            return

        # If this connection died midflight, we need to unregister this connection
        try:
            cmd_list = conn.recv_command_list(is_response=False)
        except:
            self.handle_gearman_connection_error(conn)
            return

        for cmd_tuple in cmd_list:
            cmds_to_send = self.handle_incoming_command(conn, cmd_tuple)
            self._conditional_send_commands(cmds_to_send)

    def handle_write(self, conn):
        try:
            cmd_list = conn.recv_command_list(is_response=False)
        except ConnectionError:
            self.handle_gearman_connection_error(conn)
            return

    def handle_error(self, conn):
        gearman_logger.error("Exception on connection %r" % conn)
        self.handle_gearman_connection_error(conn)
        conn.close()

    def handle_gearman_connection_error(self, conn):
        cmds_to_send = self.manager.unregister_connection(conn)
        self._conditional_send_commands(cmds_to_send)

        try:
            self.connection_list.remove(conn)
        except:
            pass

    def _conditional_send_commands(self, cmds_to_send):
        if not cmds_to_send:
            return

        for cmd_tuple in cmds_to_send:
            connection_to_send, cmd_type, cmd_args = cmd_tuple
            connection_to_send.send_command(cmd_type, cmd_args, is_response=True)

    def start(self):
        assert self.server_connection, "Not listening on any socket"

        gearman_logger.debug("Listening for connections on %r", self.server_connection.get_address())

        self.running = True
        while self.running:
            self.poll_connections_once(self.connection_list, timeout=1.0)

            cmds_to_send = self.manager.check_queues()
            self._conditional_send_commands(cmds_to_send)

        self.shutdown()

    def stop(self):
        self.running = False

    def shutdown(self):
        while self.connection_list:
            current_connection = self.connection_list.pop()
            current_connection.close()

    ###################################################################
    ### Callbacks when we receive a command from a client or worker ###
    ###################################################################
    def recv_can_do(self, worker_conn, cmd_type, cmd_args):
        return self.manager.worker_can_do(worker_conn, cmd_args['func'])

    def recv_cant_do(self, worker_conn, cmd_type, cmd_args):
        return self.manager.worker_cant_do(worker_conn, cmd_args['func'])

    def recv_reset_abilities(self, worker_conn, cmd_type, cmd_args):
        return self.manager.worker_reset_abilities(worker_conn)
 
    def recv_pre_sleep(self, worker_conn, cmd_type, cmd_args):
        return self.manager.worker_pre_sleep(worker_conn)

    def _job_from_arguments(self, client_conn, cmd_args):
        function_name = cmd_args['func']
        unique = cmd_args['unique']
        data = cmd_args['data'] 
    
        return GearmanJob(client_conn, None, function_name, unique, data)

    def recv_submit_job(self, client_conn, cmd_type, cmd_args):
        gearman_job = self._job_from_arguments(client_conn, cmd_args)
        return self.manager.client_submit_job(gearman_job, initial_priority=NO_PRIORITY, is_background=FOREGROUND_JOB)

    def recv_submit_job_high(self, client_conn, cmd_type, cmd_args):
        gearman_job = self._job_from_arguments(client_conn, cmd_args)
        return self.manager.client_submit_job(gearman_job, initial_priority=HIGH_PRIORITY, is_background=FOREGROUND_JOB)

    def recv_submit_job_low(self, client_conn, cmd_type, cmd_args):
        gearman_job = self._job_from_arguments(client_conn, cmd_args)
        return self.manager.client_submit_job(gearman_job, initial_priority=LOW_PRIORITY, is_background=FOREGROUND_JOB)

    def recv_submit_job_bg(self, client_conn, cmd_type, cmd_args):
        gearman_job = self._job_from_arguments(client_conn, cmd_args)
        return self.manager.client_submit_job(gearman_job, initial_priority=NO_PRIORITY, is_background=BACKGROUND_JOB)

    def recv_submit_job_high_bg(self, client_conn, cmd_type, cmd_args):
        gearman_job = self._job_from_arguments(client_conn, cmd_args)
        return self.manager.client_submit_job(gearman_job, initial_priority=HIGH_PRIORITY, is_background=BACKGROUND_JOB)

    def recv_submit_job_low_bg(self, client_conn, cmd_type, cmd_args):
        gearman_job = self._job_from_arguments(client_conn, cmd_args)
        return self.manager.client_submit_job(gearman_job, initial_priority=LOW_PRIORITY, is_background=BACKGROUND_JOB)

    def recv_grab_job(self, worker_conn, cmd_type, cmd_args):
        return self.manager.worker_grab_job(worker_conn, is_unique=False)

    def recv_grab_job_uniq(self, worker_conn, cmd_type, cmd_args):
        return self.manager.worker_grab_job(worker_conn, is_unique=True)

    def recv_work_status(self, worker_conn, cmd_type, cmd_args):
        return self.manager.worker_work_status(worker_conn, cmd_args)

    def recv_work_complete(self, worker_conn, cmd_type, cmd_args):
        return self.manager.worker_work_complete(worker_conn, cmd_args)

    def recv_work_fail(self, worker_conn, cmd_type, cmd_args):
        return self.manager.worker_work_fail(worker_conn, cmd_args)

    def recv_get_status(self, client_conn, cmd_type, cmd_args):
        raise NotImplementedError

    def recv_echo_req(self, gearman_conn, cmd_type, cmd_args):
        return self.manager.any_echo_req(gearman_conn, cmd_args['text'])

    def recv_set_client_id(self, worker_conn, cmd_type, cmd_args):
        raise NotImplementedError

    def recv_can_do_timeout(self, worker_conn, cmd_type, cmd_args):
        raise NotImplementedError

    def recv_all_yours(self, worker_conn, cmd_type, cmd_args):
        raise NotImplementedError

    def recv_work_exception(self, worker_conn, cmd_type, cmd_args):
        raise NotImplementedError

    def recv_option_req(self, worker_conn, cmd_type, cmd_args):
        raise NotImplementedError

    def recv_work_data(self, worker_conn, cmd_type, cmd_args):
        return self.manager.worker_work_data(worker_conn, cmd_args)

    def recv_work_warning(self, worker_conn, cmd_type, cmd_args):
        return self.manager.worker_work_warning(worker_conn, cmd_args)

    def recv_invalid_binary_command(self, any_conn, cmd_type, cmd_args):
        return [(any_conn, GEARMAN_COMMAND_ERROR, dict(err_text="Unknown command", err_code="Unknown+server+command"))]

    # Raw server responses
    def recv_server_status(self, conn, cmd_typ, cmd_args):
        all_status = self.manager.get_server_status()
        for status_line in all_status:
            conn.send_binary_string("%s\t%d\t%d\t%d\n" % (status_line['func'], status_line['num_jobs'], status_line['num_working'], status_line['num_workers']))

        conn.send_binary_string(".\n")
 
    def recv_server_version(self, conn, cmd_type, cmd_args):
        from gearman import __version__
        conn.send_binary_string("%s\n" % __version__)

    def recv_server_workers(self, conn, cmd_type, cmd_args):
        worker_status = self.manager.get_server_workers()
        for status_line in worker_status:
            conn.send_binary_string("%d %s %s : %s\n" % (status_line['fd'], status_line['host'], status_line['client_id'], " ".join(status_line['abilities'])))

        conn.send_binary_string(".\n")

    def recv_server_maxqueue(self, conn, cmd_type, cmd_args):
        raise NotImplementedError

    def recv_server_shutdown(self, conn, cmd_type, cmd_args):
        # TODO: optional "graceful" argument - close listening socket and let all existing connections complete
        self.stop()
