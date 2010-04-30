import collections
import random, sys, select, logging
import time

import gearman.util
from gearman.protocol import *
from gearman.errors import ConnectionError, InvalidWorkerState
from gearman._client_base import GearmanClientBase, GearmanConnectionHandler
from gearman.job import GearmanJob

gearman_logger = logging.getLogger("gearman.client")

POLL_TIMEOUT_IN_SECONDS = 10.0

class GearmanWorker(GearmanClientBase):
    """GearmanWorkers manage connections and ConnectionHandler

    This is the public facing gearman interface that most users should be instantiating
    All I/O will be handled by the GearmanWorker
    All state machine operations are handled on the ConnectionHandler
    """
    def __init__(self, *args, **kwargs):
        # By default we should have non-blocking sockets for a GearmanWorker
        kwargs.setdefault('blocking_timeout', 2.0)
        kwargs.setdefault('gearman_connection_handler_class', GearmanWorkerConnectionHandler) 
        super(GearmanWorker, self).__init__(*args, **kwargs)

        self.worker_abilities = {}
        self.worker_client_id = None

    def register_function(self, function_name, callback_function):
        """Register a function with gearman"""
        self.worker_abilities[function_name] = callback_function
        for connection_handler in self.connection_handlers.itervalues():
            connection_handler.set_abilities(self.worker_abilities.keys())

    def unregister_function(self, function_name):
        """Unregister a function with gearman"""
        self.worker_abilities.pop(function_name, None)
        for connection_handler in self.connection_handlers.itervalues():
            connection_handler.set_abilities(self.worker_abilities.keys())

    def set_client_id(self, client_id):
        self.worker_client_id = client_id
        for connection_handler in self.connection_handlers.itervalues():
            connection_handler.set_client_id(client_id)

    def get_alive_connections(self):
        """Return a shuffled list of connections that are alive,
        and try to reconnect to dead connections if necessary."""
        shuffled_list = list(self.connection_list)
        random.shuffle(shuffled_list)

        for conn in shuffled_list:
            if conn.is_connected():
                continue

            try:
                conn.connect()
            except ConnectionError:
                continue
            else:
                connection_handler = self.connection_handlers.get(conn)
                connection_handler.on_connect()

        alive_connections = [conn for conn in shuffled_list if conn.is_connected()]
        return alive_connections

    def on_job_execute(self, connection_handler, current_job):
        """Override this function if you'd like different exception handling behavior"""
        try:
            function_callback = self.worker_abilities[current_job.func]
            job_result = function_callback(connection_handler, current_job)
        except Exception, e:
            connection_handler.on_job_exception(current_job, e)
            return False

        connection_handler.on_job_complete(current_job, job_result)
        return True

    def work(self, poll_timeout=POLL_TIMEOUT_IN_SECONDS):
        """Loop indefinitely working tasks from all connections."""
        continue_working = True
        alive_connections = []
        while continue_working:
            alive_connections = self.get_alive_connections()
            if not alive_connections:
                raise ConnectionError("Found no alive connections in list: %r" % alive_connections)

            start_working = self.before_poll()
            if not start_working:
                break

            had_connection_activity = self.poll_connections_once(alive_connections, timeout=poll_timeout)
            continue_working = self.after_poll(had_connection_activity)

        # If we were kicked out of the worker loop, we should shutdown all our connections
        for current_connection in alive_connections:
            current_connection.close()

    def before_poll(self):
        return True

    def after_poll(self, any_activity):
        return True

class GearmanWorkerConnectionHandler(GearmanConnectionHandler):
    """GearmanWorker state machine on a per connection basis"""
    def __init__(self, *largs, **kwargs):
        super(GearmanWorkerConnectionHandler, self).__init__(*largs, **kwargs)
        self._connection_abilities = []
        self._client_id = None

        self._awaiting_job_assignment = None

    ##################################################################
    ##### Public interface methods to be called by GearmanWorker #####
    ##################################################################
    def set_abilities(self, connection_abilities_list):
        assert type(connection_abilities_list) in (list, tuple)
        self._connection_abilities = connection_abilities_list
        if self.gearman_connection.is_connected():
            self.on_abilities_update()

    def set_client_id(self, client_id):
        self._client_id = client_id
        if self.gearman_connection.is_connected():
            self.on_client_id_update()

    def on_connect(self):
        self._awaiting_job_assignment = False
    
        self.connection_abilities = set()
        
        self.on_abilities_update()
        self.on_client_id_update()

        self.send_command(GEARMAN_COMMAND_PRE_SLEEP)

    def on_abilities_update(self):
        self.send_command(GEARMAN_COMMAND_RESET_ABILITIES)
        for function_name in self._connection_abilities:
            self.send_command(GEARMAN_COMMAND_CAN_DO, function_name=function_name)

    def on_client_id_update(self):
        if self._client_id is not None:
            self.send_command(GEARMAN_COMMAND_SET_CLIENT_ID, client_id=self._client_id)

    def on_job_complete(self, current_job, job_result):
        self.send_job_complete(current_job, job_result)

    def on_job_exception(self, current_job, exception):
        self.send_job_failure(current_job)

    ###############################################################
    #### Convenience methods for typical gearman jobs to call #####
    ###############################################################

    # Send Gearman commands related to jobs
    def send_job_status(self, current_job, numerator, denominator):
        assert type(numerator) in (int, float), "Numerator must be a numeric value"
        assert type(denominator) in (int, float), "Denominator must be a numeric value"
        self.send_command(GEARMAN_COMMAND_WORK_STATUS, job_handle=current_job.handle, numerator=numerator, denominator=denominator)

    def send_job_complete(self, current_job, data):
        """Removes a job from the queue if its backgrounded"""
        self.send_command(GEARMAN_COMMAND_WORK_COMPLETE, job_handle=current_job.handle, data=data)

    def send_job_failure(self, current_job):
        """Removes a job from the queue if its backgrounded"""
        self.send_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=current_job.handle)

    def send_job_exception(self, current_job, data):
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        self.send_command(GEARMAN_COMMAND_WORK_EXCEPTION, job_handle=current_job.handle, data=data)
        self.send_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=current_job.handle)

    def send_job_data(self, current_job, data):
        self.send_command(GEARMAN_COMMAND_WORK_DATA, job_handle=current_job.handle, data=data)

    def send_job_warning(self, current_job, data):
        self.send_command(GEARMAN_COMMAND_WORK_WARNING, job_handle=current_job.handle, data=data)

    ###########################################################
    ### Callbacks when we receive a command from the server ###
    ###########################################################
    def _request_job(self):
        # We don't need to request another job if we know one's pending
        if self._awaiting_job_assignment:
            return

        self.send_command(GEARMAN_COMMAND_GRAB_JOB_UNIQ)
        self._awaiting_job_assignment = True

    def recv_noop(self):
        # If were explicitly woken up to do some jobs, we better get some work to do
        self._request_job()

        return True

    def recv_no_job(self):
        self.send_command(GEARMAN_COMMAND_PRE_SLEEP)
        self._awaiting_job_assignment = False

        return True

    def recv_job_assign_uniq(self, job_handle, function_name, unique, data):
        assert function_name in self._connection_abilities, "%s not found in %r" % (function_name, self._connection_abilities)
        if not self._awaiting_job_assignment:
            raise InvalidWorkerState("Received a job when we weren't expecting one")

        # Create a new job
        current_job = GearmanJob(self.gearman_connection, job_handle, function_name, unique, data)
        self.client_base.on_job_execute(self, current_job)

        self._awaiting_job_assignment = False

        # We'll be greedy on requesting jobs... this'll make sure we're aggressively popping things off the queue
        self._request_job()
        return True

    def recv_job_assign(self, job_handle, function_name, data):
        return self.recv_job_assign(job_handle=job_handle, function_name=function_name, unique=None, data=data)

    def recv_error(self, error_code, error_text):
        gearman_logger.error("Error from server: %s: %s" % (error_code, error_text))
        self.client_base.handle_error(self.gearman_connection)

        return False

