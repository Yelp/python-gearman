import logging
import random
import sys

from gearman.connection_manager import GearmanConnectionManager
from gearman.worker_handler import GearmanWorkerCommandHandler
from gearman.connection import ConnectionError

gearman_logger = logging.getLogger(__name__)

POLL_TIMEOUT_IN_SECONDS = 60.0

class GearmanWorker(GearmanConnectionManager):
    """
    GearmanWorker :: Interface to accept jobs from a Gearman server
    """
    command_handler_class = GearmanWorkerCommandHandler

    def __init__(self, host_list=None):
        self._worker_client_id = None
        self._worker_abilities = {}

        super(GearmanWorker, self).__init__(host_list=host_list)

        self.randomized_connections = None
        self.current_handler_holding_job_lock = None

    def _setup_handler(self, current_handler):
        current_handler.set_client_id(self._worker_client_id)
        current_handler.set_abilities(self._worker_abilities.keys())
        current_handler.set_connection_manager(self)

    ########################################################
    ##### Public methods for general GearmanWorker use #####
    ########################################################
    def register_task(self, task, callback_function):
        """Register a function with this worker

        def function_callback(calling_gearman_worker, current_job):
            return current_job.data
        """
        self._worker_abilities[task] = callback_function
        for current_handler in self.connection_to_handler_map.itervalues():
            current_handler.set_abilities(self._worker_abilities.keys())

        return task

    def unregister_task(self, task):
        """Unregister a function with worker"""
        self._worker_abilities.pop(task, None)
        for current_handler in self.connection_to_handler_map.itervalues():
            current_handler.set_abilities(self._worker_abilities.keys())

        return task

    def set_client_id(self, client_id):
        """Notify the server that we should be identified as this client ID"""
        self._worker_client_id = client_id
        for current_handler in self.connection_to_handler_map.itervalues():
            current_handler.set_client_id(self._worker_client_id)

        return client_id

    def work(self, poll_timeout=POLL_TIMEOUT_IN_SECONDS):
        """Loop indefinitely, complete tasks from all connections."""
        continue_working = True
        worker_connections = []

        def continue_while_connections_alive(any_activity):
            return self.after_poll(any_activity)

        # Shuffle our connections after the poll timeout
        while continue_working:
            self.wait_until_connection_established()

            worker_connections = self.establish_worker_connections()
            continue_working = self.poll_connections_until_stopped(worker_connections, continue_while_connections_alive, timeout=poll_timeout)

        # If we were kicked out of the worker loop, we should shutdown all our connections
        for current_connection in worker_connections:
            current_connection.close()

    def shutdown(self):
        self.current_handler_holding_job_lock = None
        super(GearmanWorker, self).shutdown()

    ###############################################################
    ## Methods to override when dealing with connection polling ##
    ##############################################################
    def establish_worker_connections(self):
        """Return a shuffled list of connections that are alive, and try to reconnect to dead connections if necessary."""
        output_connections = [current_connection for current_connection in self.connection_list if current_connection.connected]
        random.shuffle(output_connections)

        return output_connections

    def wait_until_connection_established(self, poll_timeout=None):
        # Poll to make sure we send out our request for a status update
        def stop_when_connected():
            already_connected = False
            for current_connection in self.connection_list:
                if current_connection.connected:
                    already_connected = True
                elif current_connection.disconnected:
                    self.establish_connection(current_connection)
            
            return already_connected:

        self._connection_poller.start(timeout=poll_timeout)

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
        current_connection = self.address_to_connection_map[current_job.connection]
        current_handler = self.connection_to_handler_map[current_connection]
        return current_handler

    def send_job_status(self, current_job, numerator, denominator):
        """Send a Gearman JOB_STATUS update for an inflight job"""
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
        """Send a Gearman JOB_DATA update for an inflight job"""
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_data(current_job, data=data)

    def send_job_warning(self, current_job, data):
        """Send a Gearman JOB_WARNING update for an inflight job"""
        current_handler = self._get_handler_for_job(current_job)
        current_handler.send_job_warning(current_job, data=data)

    #####################################################
    ##### Callback methods for GearmanWorkerHandler #####
    #####################################################

    def on_job_execute(self, current_job):
        try:
            function_callback = self._worker_abilities[current_job.task]
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

    def set_job_lock(self, current_handler, lock):
        """Set a worker level job lock so we don't try to hold onto 2 jobs at anytime"""
        failed_lock = bool(lock and self.current_handler_holding_job_lock is not None)
        failed_unlock = bool(not lock and self.current_handler_holding_job_lock != current_handler)

        # If we've already been locked, we should say the lock failed
        # If we're attempting to unlock something when we don't have a lock, we're in a bad state
        if failed_lock or failed_unlock:
            return False

        if lock:
            self.current_handler_holding_job_lock = current_handler
        else:
            self.current_handler_holding_job_lock = None

        return True

    def check_job_lock(self, current_handler):
        """Check to see if we hold the job lock"""
        return bool(self.current_handler_holding_job_lock == current_handler)
