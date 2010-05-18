import logging

from gearman._command_handler import GearmanCommandHandler
from gearman.errors import InvalidWorkerState
from gearman.protocol import GEARMAN_COMMAND_PRE_SLEEP, GEARMAN_COMMAND_RESET_ABILITIES, GEARMAN_COMMAND_CAN_DO, GEARMAN_COMMAND_SET_CLIENT_ID, GEARMAN_COMMAND_GRAB_JOB_UNIQ, \
    GEARMAN_COMMAND_WORK_STATUS, GEARMAN_COMMAND_WORK_COMPLETE, GEARMAN_COMMAND_WORK_FAIL, GEARMAN_COMMAND_WORK_EXCEPTION, GEARMAN_COMMAND_WORK_WARNING, GEARMAN_COMMAND_WORK_DATA

gearman_logger = logging.getLogger('gearman.worker_handler')

class GearmanWorkerCommandHandler(GearmanCommandHandler):
    """GearmanWorker state machine on a per connection basis"""

    def reset_state(self):
        self._connection_abilities = []
        self._client_id = None

        self.connection_manager.set_job_lock(self, lock=False)

    ##################################################################
    ### Public interface methods to be called by GearmanClientBase ###
    ##################################################################

    def on_connection_error(self):
        self.reset_state()

    ##################################################################
    ##### Public interface methods to be called by GearmanWorker #####
    ##################################################################

    def set_abilities(self, connection_abilities_list):
        assert type(connection_abilities_list) in (list, tuple)
        self._connection_abilities = connection_abilities_list
        self.on_abilities_update()

    def set_client_id(self, client_id):
        self._client_id = client_id
        self.on_client_id_update()

    def on_connect(self):
        self.connection_manager.set_job_lock(self, lock=False)

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

    ###############################################################
    #### Convenience methods for typical gearman jobs to call #####
    ###############################################################

    # Send Gearman commands related to jobs
    def send_job_status(self, current_job, numerator, denominator):
        assert type(numerator) in (int, float), 'Numerator must be a numeric value'
        assert type(denominator) in (int, float), 'Denominator must be a numeric value'
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

    def send_job_data(self, current_job, data):
        self.send_command(GEARMAN_COMMAND_WORK_DATA, job_handle=current_job.handle, data=data)

    def send_job_warning(self, current_job, data):
        self.send_command(GEARMAN_COMMAND_WORK_WARNING, job_handle=current_job.handle, data=data)

    ###########################################################
    ### Callbacks when we receive a command from the server ###
    ###########################################################
    def _grab_job(self):
        self.send_command(GEARMAN_COMMAND_GRAB_JOB_UNIQ)

    def _sleep(self):
        self.send_command(GEARMAN_COMMAND_PRE_SLEEP)

    def _check_job_lock(self):
        return self.connection_manager.check_job_lock(self)

    def _acquire_job_lock(self):
        return self.connection_manager.set_job_lock(self, lock=True)

    def _release_job_lock(self):
        if not self.connection_manager.set_job_lock(self, lock=False):
            raise InvalidWorkerState("Unable to release job lock for %r" % self)

        return True

    def recv_noop(self):
        # If we were woken up and we already have a job lock, do nothing
        # If we don't have a lock, try to grab a lock and grab a job
        # Otherwise our worker's already locked for work so don't try to pull another one down
        if self._check_job_lock():
            pass           
        elif self._acquire_job_lock():
            self._grab_job()
        else:
            self._sleep()

        return True

    def recv_no_job(self):
        self._release_job_lock()
        self._sleep()

        return True

    def recv_job_assign_uniq(self, job_handle, function_name, unique, data):
        assert function_name in self._connection_abilities, '%s not found in %r' % (function_name, self._connection_abilities)

        # After this point, we know this connection handler is holding onto the job lock so we don't need to acquire it again
        if not self.connection_manager.check_job_lock(self):
            raise InvalidWorkerState("Received a job when we weren't expecting one")

        # Create a new job
        gearman_job = self.connection_manager.create_job(self, job_handle, function_name, unique, data)
        self.connection_manager.on_job_execute(gearman_job)

        # We'll be greedy on requesting jobs... this'll make sure we're aggressively popping things off the queue
        self._grab_job()
        return True

    def recv_job_assign(self, job_handle, function_name, data):
        return self.recv_job_assign(job_handle=job_handle, function_name=function_name, unique=None, data=data)
