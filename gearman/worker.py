import logging
import random
import sys

from gearman import compat
from gearman.connection_manager import GearmanConnectionManager
from gearman.worker_handler import GearmanWorkerCommandHandler
from gearman.errors import ConnectionError

gearman_logger = logging.getLogger(__name__)

POLL_TIMEOUT_IN_SECONDS = 60.0

class GearmanWorker(GearmanConnectionManager):
    """
    GearmanWorker :: Interface to accept jobs from a Gearman server
    """
    command_handler_class = GearmanWorkerCommandHandler

    def __init__(self, host_list=None):
        super(GearmanWorker, self).__init__(host_list=host_list)

        self.randomized_connections = None

        self.worker_abilities = {}
        self.worker_client_id = None
        self.command_handler_holding_job_lock = None

        self._update_initial_state()

    def _update_initial_state(self):
        self.handler_initial_state['abilities'] = self.worker_abilities.keys()
        self.handler_initial_state['client_id'] = self.worker_client_id

    ########################################################
    ##### Public methods for general GearmanWorker use #####
    ########################################################
    def register_task(self, task, callback_function):
        """Register a function with this worker

        def function_callback(calling_gearman_worker, current_job):
            return current_job.data
        """
        self.worker_abilities[task] = callback_function
        self._update_initial_state()

        for command_handler in self.handler_to_connection_map.iterkeys():
            command_handler.set_abilities(self.handler_initial_state['abilities'])

        return task

    def unregister_task(self, task):
        """Unregister a function with worker"""
        self.worker_abilities.pop(task, None)
        self._update_initial_state()

        for command_handler in self.handler_to_connection_map.iterkeys():
            command_handler.set_abilities(self.handler_initial_state['abilities'])

        return task

    def set_client_id(self, client_id):
        """Notify the server that we should be identified as this client ID"""
        self.worker_client_id = client_id
        self._update_initial_state()

        for command_handler in self.handler_to_connection_map.iterkeys():
            command_handler.set_client_id(self.handler_initial_state['client_id'])

        return client_id

    def work(self, poll_timeout=POLL_TIMEOUT_IN_SECONDS):
        """Loop indefinitely, complete tasks from all connections."""
        continue_working = True
        worker_connections = []

        def continue_while_connections_alive(any_activity):
            return self.after_poll(any_activity)

        # Shuffle our connections after the poll timeout
        while continue_working:
            worker_connections = self.establish_worker_connections()
            continue_working = self.poll_connections_until_stopped(worker_connections, continue_while_connections_alive, timeout=poll_timeout)

        # If we were kicked out of the worker loop, we should shutdown all our connections
        for current_connection in worker_connections:
            current_connection.close()

    def shutdown(self):
        self.command_handler_holding_job_lock = None
        super(GearmanWorker, self).shutdown()

    ###############################################################
    ## Methods to override when dealing with connection polling ##
    ##############################################################
    def establish_worker_connections(self):
        """Return a shuffled list of connections that are alive, and try to reconnect to dead connections if necessary."""
        self.randomized_connections = list(self.connection_list)
        random.shuffle(self.randomized_connections)

        output_connections = []
        for current_connection in self.randomized_connections:
            try:
                valid_connection = self.establish_connection(current_connection)
                output_connections.append(valid_connection)
            except ConnectionError:
                pass

        return output_connections

    def after_poll(self, any_activity):
        """Polling callback to notify any outside listeners whats going on with the GearmanWorker.

        Return True to continue polling, False to exit the work loop"""
        return True

    def handle_error(self, current_connection):
        """If we discover that a connection has a problem, we better release the job lock"""
        current_handler = self.connection_to_handler_map.get(current_connection)
        if current_handler:
            self.set_job_lock(current_handler, lock=False)

        super(GearmanWorker, self).handle_error(current_connection)

    #############################################################
    ## Public methods so Gearman jobs can send Gearman updates ##
    #############################################################
    def _get_handler_for_job(self, current_job):
        return self.connection_to_handler_map[current_job.connection]

    def wait_until_updates_sent(self, multiple_gearman_jobs, poll_timeout=None):
        connection_set = set([current_job.connection for current_job in multiple_gearman_jobs])
        def continue_while_updates_pending(any_activity):
            return compat.any(current_connection.writable() for current_connection in connection_set)

        self.poll_connections_until_stopped(connection_set, continue_while_updates_pending, timeout=poll_timeout)

    def send_job_status(self, current_job, numerator, denominator, poll_timeout=None):
        """Send a Gearman JOB_STATUS update for an inflight job"""
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_status(current_job, numerator=numerator, denominator=denominator)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    def send_job_complete(self, current_job, data, poll_timeout=None):
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_complete(current_job, data=data)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    def send_job_failure(self, current_job, poll_timeout=None):
        """Removes a job from the queue if its backgrounded"""
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_failure(current_job)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    def send_job_exception(self, current_job, data, poll_timeout=None):
        """Removes a job from the queue if its backgrounded"""
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_exception(current_job, data=data)
        current_handler.send_job_failure(current_job)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    def send_job_data(self, current_job, data, poll_timeout=None):
        """Send a Gearman JOB_DATA update for an inflight job"""
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_data(current_job, data=data)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    def send_job_warning(self, current_job, data, poll_timeout=None):
        """Send a Gearman JOB_WARNING update for an inflight job"""
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_warning(current_job, data=data)

        self.wait_until_updates_sent([current_job], poll_timeout=poll_timeout)

    #####################################################
    ##### Callback methods for GearmanWorkerHandler #####
    #####################################################
    def create_job(self, command_handler, job_handle, task, unique, data):
        """Create a new job using our self.job_class"""
        current_connection = self.handler_to_connection_map[command_handler]
        return self.job_class(current_connection, job_handle, task, unique, data)

    def on_job_execute(self, current_job):
        try:
            function_callback = self.worker_abilities[current_job.task]
            job_result = function_callback(self, current_job)
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
        """Set a worker level job lock so we don't try to hold onto 2 jobs at anytime"""
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
        else:
            self.command_handler_holding_job_lock = None

        return True

    def check_job_lock(self, command_handler):
        """Check to see if we hold the job lock"""
        return bool(self.command_handler_holding_job_lock == command_handler)
