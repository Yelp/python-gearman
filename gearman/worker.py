import random, sys, select, logging
from time import time

import gearman.util
from gearman.compat import *
from gearman.protocol import *
from gearman.client import GearmanBaseClient
from gearman.job import GearmanJob

log = logging.getLogger("gearman")

class GearmanWorker(GearmanBaseClient):
    def __init__(self, *args, **kwargs):
        super(GearmanWorker, self).__init__(*args, **kwargs)
        self.abilities = {}

        # Gearman talks about jobs primarily using current_jobs
        # We're going to do the same
        self.handle_to_job_map = {}
        self.handle_to_connection_map = {}

    def register_function(self, name, func, timeout=None):
        """Register a function with gearman with an optional default timeout.
        """
        name = self.prefix + name
        self.abilities[name] = (func, timeout)

    def register_class(self, clas, name=None, decorator=None):
        """Register all the methods of a class or instance object with
        with gearman.
        
        'name' is an optional prefix for function names (name.method_name)
        """
        obj = clas
        if not isinstance(clas, type):
            clas = clas.__class__
        name = name or getattr(obj, 'name', clas.__name__)
        for k in clas.__dict__:
            v = getattr(obj, k)
            if callable(v) and k[0] != '_':
                if decorator:
                    v = decorator(v)
                self.register_function("%s.%s" % (name, k), v)

    def _set_abilities(self, conn):
        for name, args in self.abilities.iteritems():
            self.send_can_do(conn, name, args[1])

    @property
    def alive_connections(self):
        """Return a shuffled list of connections that are alive,
        and try to reconnect to dead connections if necessary."""
        random.shuffle(self.connections)
        all_dead = all(conn.is_dead for conn in self.connections)
        alive = []
        for conn in self.connections:
            if not conn.connected and (not conn.is_dead or all_dead):
                try:
                    conn.connect()
                except conn.ConnectionError:
                    continue
                else:
                    conn.sleeping = False
                    self._set_abilities(conn)
            if conn.connected:
                alive.append(conn)
        return alive

    def stop(self):
        self.working = False

    def check_connection_for_work(self, conn):
        cmd_actions = {
            GEARMAN_COMMAND_NOOP: self.on_cmd_noop,
            GEARMAN_COMMAND_NO_JOB: self.on_cmd_no_job,
            GEARMAN_COMMAND_JOB_ASSIGN: self.on_cmd_job_assign,
            GEARMAN_COMMAND_JOB_ASSIGN_UNIQ: self.on_cmd_job_assign,
            GEARMAN_COMMAND_ERROR: self.on_cmd_error,
        }

        continue_working = True
        completed_work = False

        # Kick off our processing loop and request a job
        conn.send_command(GEARMAN_COMMAND_GRAB_JOB)
        while continue_working:
            completed_work = False
            cmd_tuple = conn.recv_blocking(timeout=0.5)
            if cmd_tuple is None:
                continue

            cmd_type, cmd_args = cmd_tuple
            cmd_callback = cmd_actions.get(cmd_type, None)
            if cmd_callback is None:
                log.error("Was expecting job_assigned or no_job, received %s" % cmd_type)
                return completed_work

            continue_working, completed_work = cmd_callback(conn, cmd_type, cmd_args)
 
        return completed_work

    def work(self, stop_if=None):
        """Loop indefinitely working tasks from all connections."""
        self.working = True
        stop_if = stop_if or (lambda *a, **kw:False)
        last_job_time = time()

        while self.working:
            is_sleepy = True

            # Try to grab work from all alive connections
            for conn in self.alive_connections:
                if conn.sleeping:
                    continue

                try:
                    completed_work = self.check_connection_for_work(conn)
                except conn.ConnectionError, exc:
                    log.error("ConnectionError on %s: %s" % (conn, exc))
                else:
                    if completed_work:
                        last_job_time = time()
                        is_sleepy = False

            # If we're not sleepy, don't go to sleep 
            if not is_sleepy:
                continue

            # If no tasks were handled then sleep and wait for the server to wake us with a 'noop'
            for conn in self.alive_connections:
                if not conn.sleeping:
                    conn.send_command(GEARMAN_COMMAND_PRE_SLEEP)
                    conn.sleeping = True

            readable_conns = [c for c in self.alive_connections if c.readable()]
            rd_list, wr_list, ex_list = gearman.util.select(readable_conns, [], self.alive_connections, timeout=10)

            for c in ex_list:
                log.error("Exception on connection %s" % c)
                c.mark_dead()

            # If we actually have work to do, don't mark the connection as sleeping
            for c in rd_list:
                c.sleeping = False

            is_idle = not bool(rd_list)
            if stop_if(is_idle, last_job_time):
                self.working = False

    def register_job(self, current_job, conn):
        # Add this job to the list of jobs we're currently managing
		handle = current_job.handle
        self.handle_to_job_map[current_job.handle] = current_job
        self.handle_to_connection_map[current_job.handle] = conn

    def unregister_job(self, current_job):
		handle = current_job.handle
	    del self.handle_to_job_map[handle]
        del self.handle_to_connection_map[handle]
    
    # Gearman worker callbacks when we have job stuff
    def on_job_execute(self, current_job, function_callback):
        try:
            job_result = function_callback(current_job)
        except Exception, caught_exception:
	        self.send_job_failure(current_job)
        else:
	        self.send_job_complete(current_job, job_result)

    # Gearman worker callbacks when we receive a command from the server
    def on_cmd_noop(self, conn, cmd_type, cmd_args):
        continue_working = True
        completed_work = False
        return continue_working, completed_work

    def on_cmd_no_job(self, conn, cmd_type, cmd_args):
        continue_working = False
        completed_work = False
        return continue_working, completed_work

    def on_cmd_error(self, conn, cmd_type, cmd_args):
        log.error("Error from server: %s: %s" % (cmd_args['err_code'], cmd_args['err_text']))
        conn.mark_dead()

        continue_working = False
        completed_work = False
        return continue_working, completed_work

    def on_cmd_job_assign(self, conn, cmd_type, cmd_args):
        continue_working = False
        completed_work = False

        handle = cmd_args['handle']
        function_name = cmd_args['func']
        unique = cmd_args.get('unique', None)
        data = cmd_args['data']

        current_job = GearmanJob(handle, function_name, unique, data, gearman_worker=self)
        self.register_job(current_job, conn)

        function_name = current_job.func
        try:
            function_callback = self.abilities[function_name][0]
        except KeyError:
            log.error("Received work for unknown function %s" % cmd_args)
            completed_work = False
            return continue_working, completed_work

		self.on_job_execute(current_job, function_callback)

        # Don't forget to remove this job
        self.unregister_job(current_job)
        completed_work = True
        return continue_working, completed_work

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
        assert handle in self.handle_to_connection_map, "Could not find job for this worker: %r" % handle
        current_connection = self.handle_to_connection_map[handle]

        assert bool(current_connection) and not current_connection.is_dead, "Could not find a valid connection for this job: %r" % handle
        full_cmd_args = partial_cmd_args.copy()
        full_cmd_args['handle'] = handle
        current_connection.send_command_blocking(cmd_type, full_cmd_args)