import logging
import random
import sys

from gearman._connection_manager import GearmanConnectionManager
from gearman.job import GearmanJob
from gearman.errors import ConnectionError, ServerUnavailable
from gearman.worker_handler import GearmanWorkerCommandHandler

gearman_logger = logging.getLogger('gearman.worker')

POLL_TIMEOUT_IN_SECONDS = 60.0

class GearmanWorker(GearmanConnectionManager):
    """GearmanWorkers manage connections and CommandHandlers

    This is the public facing gearman interface that most users should be instantiating
    All I/O will be handled by the GearmanWorker
    All state machine operations are handled on the CommandHandler
    """
    command_handler_class = GearmanWorkerCommandHandler
    gearman_job_class = GearmanJob

    def __init__(self, *args, **kwargs):
        # By default we should have non-blocking sockets for a GearmanWorker
        kwargs.setdefault('blocking_timeout', 0.0)
        super(GearmanWorker, self).__init__(*args, **kwargs)

        self.worker_abilities = {}
        self.worker_client_id = None
        self.command_handler_holding_job_lock = None

        self._update_initial_state()

    def _update_initial_state(self):
        self.handler_initial_state['abilities'] = self.worker_abilities.keys()
        self.handler_initial_state['client_id'] = self.worker_client_id

    def register_function(self, function_name, callback_function):
        """Register a function with gearman"""
        self.worker_abilities[function_name] = callback_function
        self._update_initial_state()

        for command_handler in self.handler_to_connection_map.iterkeys():
            command_handler.set_abilities(self.handler_initial_state['abilities'])

        return function_name

    def unregister_function(self, function_name):
        """Unregister a function with gearman"""
        self.worker_abilities.pop(function_name, None)
        self._update_initial_state()

        for command_handler in self.handler_to_connection_map.iterkeys():
            command_handler.set_abilities(self.handler_initial_state['abilities'])

        return function_name

    def set_client_id(self, client_id):
        self.worker_client_id = client_id
        self._update_initial_state()

        for command_handler in self.handler_to_connection_map.iterkeys():
            command_handler.set_client_id(self.handler_initial_state['client_id'])

        return client_id

    def get_live_connections(self):
        """Return a shuffled list of connections that are alive,
        and try to reconnect to dead connections if necessary."""
        shuffled_list = list(self.connection_list)
        random.shuffle(shuffled_list)

        for current_connection in shuffled_list:
            self.attempt_connect(current_connection)

        live_connections = [conn for conn in shuffled_list if conn.is_connected()]
        if not live_connections:
            raise ServerUnavailable('Found no valid connections in list: %r' % shuffled_list)

        return live_connections

    def create_job(self, command_handler, job_handle, function_name, unique, data):
        current_connection = self.handler_to_connection_map[command_handler]
        return self.gearman_job_class(current_connection, job_handle, function_name, unique, data)

    def on_job_execute(self, current_job):
        try:
            function_callback = self.worker_abilities[current_job.func]
            job_result = function_callback(current_job)
        except Exception:
            return self.on_job_exception(current_job, sys.exc_info())

        return self.on_job_complete(current_job, job_result)

    def on_job_exception(self, current_job, exc_info):
        self.send_job_failure(current_job)
        return False

    def on_job_complete(self, current_job, job_result):
        self.send_job_complete(current_job, job_result)
        return True

    def set_job_lock(self, command_handler, lock):
        if command_handler not in self.handler_to_connection_map:
            return False

        failed_lock = bool(lock and self.command_handler_holding_job_lock is not None)
        failed_unlock = bool(not lock and self.command_handler_holding_job_lock != command_handler)

        # If we've already been locked, we should say the lock failed
        # If we're attempting to unlock something when we don't have a lock, we're in a bad state
        if failed_lock or failed_unlock:
            return False

        if lock:
            self.command_handler_holding_job_lock = command_handler
        elif not lock:
            self.command_handler_holding_job_lock = None

        return True

    def check_job_lock(self, command_handler):
        return bool(self.command_handler_holding_job_lock == command_handler)

    # Send Gearman commands related to jobs
    def _get_handler_for_job(self, current_job):
        return self.connection_to_handler_map[current_job.conn]

    def send_job_status(self, current_job, numerator, denominator):
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_status(current_job, numerator=numerator, denominator=denominator)

    def send_job_complete(self, current_job, data):
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_complete(current_job, data=data)

    def send_job_failure(self, current_job):
        """Removes a job from the queue if its backgrounded"""
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_failure(current_job)

    def send_job_exception(self, current_job, data):
        """Removes a job from the queue if its backgrounded"""
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_exception(current_job, data=data)
        current_handler.send_job_failure(current_job)

    def send_job_data(self, current_job, data):
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_data(current_job, data=data)

    def send_job_warning(self, current_job, data):
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_warning(current_job, data=data)

    def work(self, poll_timeout=POLL_TIMEOUT_IN_SECONDS):
        """Loop indefinitely working tasks from all connections."""
        continue_working = True
        live_connections = []

        def instance_method_wrapper(any_activity, callback_data):
            return self.after_poll

        while continue_working:
            live_connections = self.get_live_connections()
            continue_working = self.poll_connections_until_stopped(live_connections, instance_method_wrapper, timeout=poll_timeout)

        # If we were kicked out of the worker loop, we should shutdown all our connections
        for current_connection in live_connections:
            current_connection.close()

    def after_poll(self, any_activity, callback_data=None):
        return True

    def shutdown(self):
        self.command_handler_holding_job_lock = None
        super(GearmanWorker, self).shutdown(current_connection)

    def handle_error(self, current_connection):
        current_handler = self.connection_to_handler_map.get(current_connection)
        if current_handler:
            self.set_job_lock(current_handler, lock=False)

        super(GearmanWorker, self).handle_error(current_connection)
