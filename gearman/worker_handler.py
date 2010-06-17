import logging

from gearman.command_handler import GearmanCommandHandler
from gearman.errors import InvalidWorkerState
from gearman.protocol import GEARMAN_COMMAND_PRE_SLEEP, GEARMAN_COMMAND_RESET_ABILITIES, GEARMAN_COMMAND_CAN_DO, GEARMAN_COMMAND_SET_CLIENT_ID, GEARMAN_COMMAND_GRAB_JOB_UNIQ, \
    GEARMAN_COMMAND_WORK_STATUS, GEARMAN_COMMAND_WORK_COMPLETE, GEARMAN_COMMAND_WORK_FAIL, GEARMAN_COMMAND_WORK_EXCEPTION, GEARMAN_COMMAND_WORK_WARNING, GEARMAN_COMMAND_WORK_DATA

gearman_logger = logging.getLogger(__name__)

class GearmanWorkerCommandHandler(GearmanCommandHandler):
    """GearmanWorker state machine on a per connection basis

    A worker can be in the following distinct states:
        SLEEP         -> Doing nothing, can be awoken
        AWAKE         -> Transitional state (for NOOP)
        AWAITING_JOB  -> Holding worker level job lock and awaiting a server response
        EXECUTING_JOB -> Transitional state (for ASSIGN_JOB)
    """
    def __init__(self, connection_manager=None):
        super(GearmanWorkerCommandHandler, self).__init__(connection_manager=connection_manager)

        self._handler_abilities = []
        self._client_id = None

    def initial_state(self, abilities=None, client_id=None):
        self.set_client_id(client_id)
        self.set_abilities(abilities)

        self._sleep()

    ##################################################################
    ##### Public interface methods to be called by GearmanWorker #####
    ##################################################################
    def set_abilities(self, connection_abilities_list):
        assert type(connection_abilities_list) in (list, tuple)
        self._handler_abilities = connection_abilities_list

        self.send_command(GEARMAN_COMMAND_RESET_ABILITIES)
        for task in self._handler_abilities:
            self.send_command(GEARMAN_COMMAND_CAN_DO, task=task)

    def set_client_id(self, client_id):
        self._client_id = client_id

        if self._client_id is not None:
            self.send_command(GEARMAN_COMMAND_SET_CLIENT_ID, client_id=self._client_id)

    ###############################################################
    #### Convenience methods for typical gearman jobs to call #####
    ###############################################################
    def send_job_status(self, current_job, numerator, denominator):
        assert type(numerator) in (int, float), 'Numerator must be a numeric value'
        assert type(denominator) in (int, float), 'Denominator must be a numeric value'
        self.send_command(GEARMAN_COMMAND_WORK_STATUS, job_handle=current_job.handle, numerator=str(numerator), denominator=str(denominator))

    def send_job_complete(self, current_job, data):
        """Removes a job from the queue if its backgrounded"""
        self.send_command(GEARMAN_COMMAND_WORK_COMPLETE, job_handle=current_job.handle, data=self.encode_data(data))

    def send_job_failure(self, current_job):
        """Removes a job from the queue if its backgrounded"""
        self.send_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=current_job.handle)

    def send_job_exception(self, current_job, data):
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        self.send_command(GEARMAN_COMMAND_WORK_EXCEPTION, job_handle=current_job.handle, data=self.encode_data(data))

    def send_job_data(self, current_job, data):
        self.send_command(GEARMAN_COMMAND_WORK_DATA, job_handle=current_job.handle, data=self.encode_data(data))

    def send_job_warning(self, current_job, data):
        self.send_command(GEARMAN_COMMAND_WORK_WARNING, job_handle=current_job.handle, data=self.encode_data(data))

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
        """Transition from being SLEEP --> AWAITING_JOB / SLEEP

          AWAITING_JOB -> AWAITING_JOB :: Noop transition, we're already awaiting a job
        SLEEP -> AWAKE -> AWAITING_JOB :: Transition if we can acquire the worker job lock
        SLEEP -> AWAKE -> SLEEP        :: Transition if we can NOT acquire a worker job lock
        """
        if self._check_job_lock():
            pass
        elif self._acquire_job_lock():
            self._grab_job()
        else:
            self._sleep()

        return True

    def recv_no_job(self):
        """Transition from being AWAITING_JOB --> SLEEP

        AWAITING_JOB -> SLEEP :: Always transition to sleep if we have nothing to do
        """
        self._release_job_lock()
        self._sleep()

        return True

    def recv_job_assign_uniq(self, job_handle, task, unique, data):
        """Transition from being AWAITING_JOB --> EXECUTE_JOB --> SLEEP

        AWAITING_JOB -> EXECUTE_JOB -> SLEEP :: Always transition once we're given a job
        """
        assert task in self._handler_abilities, '%s not found in %r' % (task, self._handler_abilities)

        # After this point, we know this connection handler is holding onto the job lock so we don't need to acquire it again
        if not self.connection_manager.check_job_lock(self):
            raise InvalidWorkerState("Received a job when we weren't expecting one")

        gearman_job = self.connection_manager.create_job(self, job_handle, task, unique, self.decode_data(data))

        # Create a new job
        self.connection_manager.on_job_execute(gearman_job)

        # Release the job lock once we're doing and go back to sleep
        self._release_job_lock()
        self._sleep()

        return True

    def recv_job_assign(self, job_handle, task, data):
        """JOB_ASSIGN and JOB_ASSIGN_UNIQ are essentially the same"""
        return self.recv_job_assign(job_handle=job_handle, task=task, unique=None, data=data)
