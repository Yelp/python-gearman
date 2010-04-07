import collections
import random, sys, select, logging
import time

import gearman.util
from gearman.compat import *
from gearman.protocol import *
from gearman.errors import ConnectionError
from gearman._client_base import GearmanClientBase
from gearman.job import GearmanJob

log = logging.getLogger("gearman")

POLL_INTERVAL_IN_SECONDS = 1.0
SLEEP_INTERVAL_IN_SECONDS = 10.0

class GearmanWorker(GearmanClientBase):
    client_type = "worker"

    def __init__(self, *args, **kwargs):
        # By default we should have non-blocking sockets for a GearmanWorker
        kwargs.setdefault('blocking_timeout', 2.0)
        super(GearmanWorker, self).__init__(*args, **kwargs)
        self.abilities = {}
        self.conns_awaiting_job_assignment = set()

        self.command_handlers = {
            GEARMAN_COMMAND_NOOP: self.recv_noop,
            GEARMAN_COMMAND_NO_JOB: self.recv_no_job,
            GEARMAN_COMMAND_JOB_ASSIGN: self.recv_job_assign,
            GEARMAN_COMMAND_JOB_ASSIGN_UNIQ: self.recv_job_assign_uniq,
            GEARMAN_COMMAND_ERROR: self.recv_error,
        }

    def register_function(self, job_name, callback_function):
        """Register a function with gearman"""
        self.abilities[job_name] = callback_function

    def unregister_function(self, job_name):
        pass

    def _set_abilities(self, conn):
        for name, args in self.abilities.iteritems():
            self.send_can_do(conn, name)

    def set_client_id(self, client_id):
        for conn in self.get_alive_connections():
            conn.send_command(GEARMAN_COMMAND_SET_CLIENT_ID, data=client_id)

    def get_alive_connections(self):
        """Return a shuffled list of connections that are alive,
        and try to reconnect to dead connections if necessary."""
        random.shuffle(self.connection_list)

        alive = []
        for conn in self.connection_list:
            if not conn.is_connected():
                try:
                    conn.connect()
                except ConnectionError:
                    continue
                else:
                    self._set_abilities(conn)
                    conn.send_command(GEARMAN_COMMAND_PRE_SLEEP)

            if conn.is_connected():
                alive.append(conn)

        return alive

    def stop(self):
        self.working = False

    def work(self, stop_if=None, poll_interval=POLL_INTERVAL_IN_SECONDS, sleep_interval=SLEEP_INTERVAL_IN_SECONDS):
        """Loop indefinitely working tasks from all connections."""
        self.working = continue_working = True
        stop_if = stop_if or (lambda *a, **kw:False)
        last_job_time = time.time()

        while self.working and continue_working:
            had_connection_activity = self.poll_connections_once(self.get_alive_connections(), timeout=poll_interval)

            is_idle = not had_connection_activity
            continue_working = not bool(stop_if(is_idle, last_job_time))

        # If we were kicked out of the worker loop, we should shutdown all our connections
        for current_connection in self.get_alive_connections():
            current_connection.close()

    def handle_read(self, conn):
        """For our worker, we'll want to do blocking calls on processing out commands"""
        self.conns_awaiting_job_assignment.discard(conn)
        
        cmd_list = conn.recv_command_list()
        for cmd_tuple in cmd_list:
            completed_work = self.handle_incoming_command(conn, cmd_tuple)
            if completed_work is False:
                break

        self.conns_awaiting_job_assignment.discard(conn)

    def on_job_execute(self, current_job):
        """Override this function if you'd like different exception handling behavior"""
        try:
            job_result = self.run_job(current_job)
        except Exception:
            self.send_job_failure(current_job)
            return False

        self.send_job_complete(current_job, job_result)
        return True

    def run_job(self, current_job):
        function_callback = self.abilities[current_job.func]
        return function_callback(self, current_job)

    ##################################################################
    ##### Worker callbacks when we send a command from the server ####
    ##################################################################
    def send_can_do(self, conn, function_name, timeout=None):
        if timeout is None:
            cmd_type = GEARMAN_COMMAND_CAN_DO
            cmd_args = dict(func=function_name)
        else:
            cmd_type = GEARMAN_COMMAND_CAN_DO_TIMEOUT
            cmd_args = dict(func=function_name, timeout=timeout)

        conn.send_command(cmd_type, cmd_args)

    # Send Gearman commands related to jobs
    def send_job_status(self, current_job, numerator, denominator):
        assert type(numerator) in (int, float), "Numerator must be a numeric value"
        assert type(denominator) in (int, float), "Denominator must be a numeric value"
        self.send_command_for_job(current_job, GEARMAN_COMMAND_WORK_STATUS, numerator=numerator, denominator=denominator)

    def send_job_complete(self, current_job, data):
        """Removes a job from the queue if its backgrounded"""
        self.send_command_for_job(current_job, GEARMAN_COMMAND_WORK_COMPLETE, data=data)

    def send_job_failure(self, current_job):
        """Removes a job from the queue if its backgrounded"""
        self.send_command_for_job(current_job, GEARMAN_COMMAND_WORK_FAIL)

    def send_job_exception(self, current_job, data):
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        self.send_command_for_job(current_job, GEARMAN_COMMAND_WORK_EXCEPTION, data=data)
        self.send_command_for_job(current_job, GEARMAN_COMMAND_WORK_FAIL)

    def send_job_data(self, current_job, data):
        self.send_command_for_job(current_job, GEARMAN_COMMAND_WORK_DATA, data=data)

    def send_job_warning(self, current_job, data):
        self.send_command_for_job(current_job, GEARMAN_COMMAND_WORK_WARNING, data=data)

    def send_command_for_job(self, current_job, cmd_type, **partial_cmd_args):
        handle = current_job.handle
        current_connection = current_job.conn
        assert bool(current_connection) and current_connection.is_connected(), "Could not find a valid connection for this job: %r" % handle

        full_cmd_args = partial_cmd_args.copy()
        full_cmd_args['handle'] = handle
        current_connection.send_command(cmd_type, full_cmd_args)

    ###########################################################
    ### Callbacks when we receive a command from the server ###
    ###########################################################
    def request_job(self, conn):
        if conn not in self.conns_awaiting_job_assignment:
            conn.send_command(GEARMAN_COMMAND_GRAB_JOB_UNIQ)
            self.conns_awaiting_job_assignment.add(conn)

    def recv_noop(self, conn, cmd_type, cmd_args):
        # If were explicitly woken up to do some jobs, we better get some work to do
        self.request_job(conn)

        return True

    def recv_no_job(self, conn, cmd_type, cmd_args):
        conn.send_command(GEARMAN_COMMAND_PRE_SLEEP)
        self.conns_awaiting_job_assignment.discard(conn)

        return False

    def recv_error(self, conn, cmd_type, cmd_args):
        log.error("Error from server: %s: %s" % (cmd_args['err_code'], cmd_args['err_text']))
        conn.close()
        return False

    def recv_job_assign(self, conn, cmd_type, cmd_args):
        handle = cmd_args['handle']
        function_name = cmd_args['func']
        unique = cmd_args.get('unique', None)
        data = cmd_args['data']

        assert function_name in self.abilities, "%s not found in %r" % (function_name, self.abilities.keys())

        # Create a new job
        current_job = GearmanJob(conn, handle, function_name, unique, data)

        self.on_job_execute(current_job)

        self.request_job(conn)
        return False

    def recv_job_assign_uniq(self, conn, cmd_type, cmd_args):
        return self.recv_job_assign(conn, cmd_args, cmd_args)
