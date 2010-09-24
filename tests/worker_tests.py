import collections
from gearman import compat
import unittest

from gearman.worker import GearmanWorker
from gearman.worker_handler import GearmanWorkerCommandHandler

from gearman.errors import ServerUnavailable, InvalidWorkerState
from gearman.protocol import get_command_name, GEARMAN_COMMAND_RESET_ABILITIES, GEARMAN_COMMAND_CAN_DO, GEARMAN_COMMAND_SET_CLIENT_ID, \
    GEARMAN_COMMAND_NOOP, GEARMAN_COMMAND_PRE_SLEEP, GEARMAN_COMMAND_NO_JOB, GEARMAN_COMMAND_GRAB_JOB_UNIQ, GEARMAN_COMMAND_JOB_ASSIGN_UNIQ, \
    GEARMAN_COMMAND_WORK_STATUS, GEARMAN_COMMAND_WORK_FAIL, GEARMAN_COMMAND_WORK_COMPLETE, GEARMAN_COMMAND_WORK_DATA, GEARMAN_COMMAND_WORK_EXCEPTION, GEARMAN_COMMAND_WORK_WARNING

from tests._core_testing import _GearmanAbstractTest, MockGearmanConnectionManager, MockGearmanConnection

class MockGearmanWorker(MockGearmanConnectionManager, GearmanWorker):
    def __init__(self, *largs, **kwargs):
        super(MockGearmanWorker, self).__init__(*largs, **kwargs)
        self.worker_job_queues = compat.defaultdict(collections.deque)

    def on_job_execute(self, current_job):
        current_handler = self.connection_to_handler_map[current_job.connection]
        self.worker_job_queues[current_handler].append(current_job)

class _GearmanAbstractWorkerTest(_GearmanAbstractTest):
    connection_manager_class = MockGearmanWorker
    command_handler_class = GearmanWorkerCommandHandler

    def setup_command_handler(self):
        super(_GearmanAbstractWorkerTest, self).setup_command_handler()
        self.assert_sent_abilities([])
        self.assert_sent_command(GEARMAN_COMMAND_PRE_SLEEP)

    def assert_sent_abilities(self, expected_abilities):
        observed_abilities = set()

        self.assert_sent_command(GEARMAN_COMMAND_RESET_ABILITIES)
        for ability in expected_abilities:
            cmd_type, cmd_args = self.connection._outgoing_commands.popleft()

            self.assertEqual(get_command_name(cmd_type), get_command_name(GEARMAN_COMMAND_CAN_DO))
            observed_abilities.add(cmd_args['task'])

        self.assertEqual(observed_abilities, set(expected_abilities))

    def assert_sent_client_id(self, expected_client_id):
        self.assert_sent_command(GEARMAN_COMMAND_SET_CLIENT_ID, client_id=expected_client_id)

class WorkerTest(_GearmanAbstractWorkerTest):
    """Test the public worker interface"""
    def test_registering_functions(self):
        # Tests that the abilities were set on the GearmanWorker AND the GearmanWorkerCommandHandler
        # Does NOT test that commands were actually sent out as that is tested in GearmanWorkerCommandHandlerInterfaceTest.test_set_abilities
        def fake_callback_one(worker_command_handler, current_job):
            pass

        def fake_callback_two(worker_command_handler, current_job):
            pass

        # Register a single callback
        self.connection_manager.register_task('fake_callback_one', fake_callback_one)
        self.failUnless('fake_callback_one' in self.connection_manager.worker_abilities)
        self.failIf('fake_callback_two' in self.connection_manager.worker_abilities)
        self.assertEqual(self.connection_manager.worker_abilities['fake_callback_one'], fake_callback_one)
        self.assertEqual(self.command_handler._handler_abilities, ['fake_callback_one'])

        # Register another callback and make sure the command_handler sees the same functions
        self.connection_manager.register_task('fake_callback_two', fake_callback_two)
        self.failUnless('fake_callback_one' in self.connection_manager.worker_abilities)
        self.failUnless('fake_callback_two' in self.connection_manager.worker_abilities)
        self.assertEqual(self.connection_manager.worker_abilities['fake_callback_one'], fake_callback_one)
        self.assertEqual(self.connection_manager.worker_abilities['fake_callback_two'], fake_callback_two)
        self.assertEqual(self.command_handler._handler_abilities, ['fake_callback_one', 'fake_callback_two'])

        # Unregister a callback and make sure the command_handler sees the same functions
        self.connection_manager.unregister_task('fake_callback_one')
        self.failIf('fake_callback_one' in self.connection_manager.worker_abilities)
        self.failUnless('fake_callback_two' in self.connection_manager.worker_abilities)
        self.assertEqual(self.connection_manager.worker_abilities['fake_callback_two'], fake_callback_two)
        self.assertEqual(self.command_handler._handler_abilities, ['fake_callback_two'])

    def test_setting_client_id(self):
        new_client_id = 'HELLO'

        # Make sure nothing is set
        self.assertEqual(self.connection_manager.worker_client_id, None)
        self.assertEqual(self.command_handler._client_id, None)

        self.connection_manager.set_client_id(new_client_id)

        # Make sure both the client and the connection handler reflect the new state
        self.assertEqual(self.connection_manager.worker_client_id, new_client_id)
        self.assertEqual(self.command_handler._client_id, new_client_id)

    def test_establish_worker_connections(self):
        self.connection_manager.connection_list = []
        self.connection_manager.command_handlers = {}

        # Spin up a bunch of imaginary gearman connections
        good_connection = MockGearmanConnection()
        good_connection.connect()
        good_connection._fail_on_bind = False

        failed_then_retried_connection = MockGearmanConnection()
        failed_then_retried_connection._fail_on_bind = False

        failed_connection = MockGearmanConnection()
        failed_connection._fail_on_bind = True

        # Register all our connections
        self.connection_manager.connection_list = [good_connection, failed_then_retried_connection, failed_connection]

        # The only alive connections should be the ones that ultimately be connection.connected
        alive_connections = self.connection_manager.establish_worker_connections()
        self.assertTrue(good_connection in alive_connections)
        self.assertTrue(failed_then_retried_connection in alive_connections)
        self.assertFalse(failed_connection in alive_connections)

    def test_establish_worker_connections_dead(self):
        self.connection_manager.connection_list = []
        self.connection_manager.command_handlers = {}

        # We have no connections so there will never be any work to do
        self.assertRaises(ServerUnavailable, self.connection_manager.work)

        # We were started with a dead connection, make sure we bail again
        dead_connection = MockGearmanConnection()
        dead_connection._fail_on_bind = True
        dead_connection.connected = False
        self.connection_manager.connection_list = [dead_connection]

        self.assertRaises(ServerUnavailable, self.connection_manager.work)


class WorkerCommandHandlerInterfaceTest(_GearmanAbstractWorkerTest):
    """Test the public interface a GearmanWorker may need to call in order to update state on a GearmanWorkerCommandHandler"""

    def test_on_connect(self):
        expected_abilities = ['function_one', 'function_two', 'function_three']
        expected_client_id = 'my_client_id'

        self.connection.connected = False

        self.connection_manager.set_client_id(expected_client_id)
        self.connection_manager.unregister_task('__test_ability__')
        for task in expected_abilities:
            self.connection_manager.register_task(task, None)

        # We were disconnected, connect and wipe pending commands
        self.connection_manager.establish_connection(self.connection)

        # When we attempt a new connection, make sure we get a new command handler
        self.assertNotEquals(self.command_handler, self.connection_manager.connection_to_handler_map[self.connection])

        self.assert_sent_client_id(expected_client_id)
        self.assert_sent_abilities(expected_abilities)
        self.assert_sent_command(GEARMAN_COMMAND_PRE_SLEEP)
        self.assert_no_pending_commands()

    def test_set_abilities(self):
        expected_abilities = ['function_one', 'function_two', 'function_three']

        # We were disconnected, connect and wipe pending commands
        self.command_handler.set_abilities(expected_abilities)
        self.assert_sent_abilities(expected_abilities)
        self.assert_no_pending_commands()

    def test_set_client_id(self):
        expected_client_id = 'my_client_id'

        handler_initial_state = {}
        handler_initial_state['abilities'] = []
        handler_initial_state['client_id'] = None

        # We were disconnected, connect and wipe pending commands
        self.command_handler.set_client_id(expected_client_id)
        self.assert_sent_client_id(expected_client_id)
        self.assert_no_pending_commands()

    def test_send_functions(self):
        current_job = self.generate_job()

        # Test GEARMAN_COMMAND_WORK_STATUS
        self.command_handler.send_job_status(current_job, 0, 1)
        self.assert_sent_command(GEARMAN_COMMAND_WORK_STATUS, job_handle=current_job.handle, numerator='0', denominator='1')

        # Test GEARMAN_COMMAND_WORK_COMPLETE
        self.command_handler.send_job_complete(current_job, 'completion data')
        self.assert_sent_command(GEARMAN_COMMAND_WORK_COMPLETE, job_handle=current_job.handle, data='completion data')

        # Test GEARMAN_COMMAND_WORK_FAIL
        self.command_handler.send_job_failure(current_job)
        self.assert_sent_command(GEARMAN_COMMAND_WORK_FAIL, job_handle=current_job.handle)

        # Test GEARMAN_COMMAND_WORK_EXCEPTION
        self.command_handler.send_job_exception(current_job, 'exception data')
        self.assert_sent_command(GEARMAN_COMMAND_WORK_EXCEPTION, job_handle=current_job.handle, data='exception data')

        # Test GEARMAN_COMMAND_WORK_DATA
        self.command_handler.send_job_data(current_job, 'job data')
        self.assert_sent_command(GEARMAN_COMMAND_WORK_DATA, job_handle=current_job.handle, data='job data')

        # Test GEARMAN_COMMAND_WORK_WARNING
        self.command_handler.send_job_warning(current_job, 'job warning')
        self.assert_sent_command(GEARMAN_COMMAND_WORK_WARNING, job_handle=current_job.handle, data='job warning')

class WorkerCommandHandlerStateMachineTest(_GearmanAbstractWorkerTest):
    """Test multiple state transitions within a GearmanWorkerCommandHandler

    End to end tests without a server
    """
    connection_manager_class = MockGearmanWorker
    command_handler_class = GearmanWorkerCommandHandler

    def setup_connection_manager(self):
        super(WorkerCommandHandlerStateMachineTest, self).setup_connection_manager()
        self.connection_manager.register_task('__test_ability__', None)

    def setup_command_handler(self):
        super(_GearmanAbstractWorkerTest, self).setup_command_handler()
        self.assert_sent_abilities(['__test_ability__'])
        self.assert_sent_command(GEARMAN_COMMAND_PRE_SLEEP)

    def test_wakeup_work(self):
        self.move_to_state_wakeup()

        self.move_to_state_job_assign_uniq(self.generate_job_dict())

        self.move_to_state_wakeup()

        self.move_to_state_no_job()

    def test_wakeup_sleep_wakup_work(self):
        self.move_to_state_wakeup()

        self.move_to_state_no_job()

        self.move_to_state_wakeup()

        self.move_to_state_job_assign_uniq(self.generate_job_dict())

        self.move_to_state_wakeup()

        self.move_to_state_no_job()

    def test_multiple_wakeup_then_no_work(self):
        # Awaken the state machine... then give it no work
        self.move_to_state_wakeup()

        for _ in range(5):
            self.command_handler.recv_command(GEARMAN_COMMAND_NOOP)

        self.assert_job_lock(is_locked=True)

        # Pretend like the server has no work... do nothing
        # Moving to state NO_JOB will make sure there's only 1 item on the queue
        self.move_to_state_no_job()

    def test_multiple_work(self):
        self.move_to_state_wakeup()

        self.move_to_state_job_assign_uniq(self.generate_job_dict())

        self.move_to_state_wakeup()

        self.move_to_state_job_assign_uniq(self.generate_job_dict())

        self.move_to_state_wakeup()

        self.move_to_state_job_assign_uniq(self.generate_job_dict())

        self.move_to_state_wakeup()

        # After this job completes, we're going to greedily ask for more jobs
        self.move_to_state_no_job()

    def test_worker_already_locked(self):
        other_connection = MockGearmanConnection()
        self.connection_manager.connection_list.append(other_connection)
        self.connection_manager.establish_connection(other_connection)

        other_handler = self.connection_manager.connection_to_handler_map[other_connection]
        other_handler.recv_command(GEARMAN_COMMAND_NOOP)

        # Make sure other handler has a lock
        self.assertEqual(self.connection_manager.command_handler_holding_job_lock, other_handler)

        # Make sure OUR handler has nothing incoming
        self.assert_no_pending_commands()

        # Make sure we try to grab a job but fail...so go back to sleep
        self.command_handler.recv_command(GEARMAN_COMMAND_NOOP)
        self.assert_sent_command(GEARMAN_COMMAND_PRE_SLEEP)

        # Make sure other handler still has lock
        self.assertEqual(self.connection_manager.command_handler_holding_job_lock, other_handler)

        # Make the other handler release its lock
        other_handler.recv_command(GEARMAN_COMMAND_NO_JOB)

        # Ensure that the lock has been freed
        self.assert_job_lock(is_locked=False)

        # Try to do work after we have our lock released
        self.move_to_state_wakeup()

        self.move_to_state_job_assign_uniq(self.generate_job_dict())

        self.move_to_state_wakeup()

        self.move_to_state_no_job()

    def move_to_state_wakeup(self):
        self.assert_no_pending_commands()
        self.assert_job_lock(is_locked=False)

        self.command_handler.recv_command(GEARMAN_COMMAND_NOOP)

    def move_to_state_no_job(self):
        """Move us to the NO_JOB state...

        1) We should've most recently sent only a single GEARMAN_COMMAND_GRAB_JOB_UNIQ
        2) We should be awaiting job assignment
        3) Once we receive a NO_JOB, we should say we're going back to sleep"""
        self.assert_awaiting_job()

        self.command_handler.recv_command(GEARMAN_COMMAND_NO_JOB)

        # We should be asleep... which means no pending jobs and we're not awaiting job assignment
        self.assert_sent_command(GEARMAN_COMMAND_PRE_SLEEP)
        self.assert_no_pending_commands()
        self.assert_job_lock(is_locked=False)

    def move_to_state_job_assign_uniq(self, fake_job):
        """Move us to the JOB_ASSIGN_UNIQ state...

        1) We should've most recently sent only a single GEARMAN_COMMAND_GRAB_JOB_UNIQ
        2) We should be awaiting job assignment
        3) The job we receive should be the one we expected"""
        self.assert_awaiting_job()

        ### NOTE: This recv_command does NOT send out a GEARMAN_COMMAND_JOB_COMPLETE or GEARMAN_COMMAND_JOB_FAIL
        ###           as we're using a MockGearmanConnectionManager with a method that only queues the job
        self.command_handler.recv_command(GEARMAN_COMMAND_JOB_ASSIGN_UNIQ, **fake_job)

        current_job = self.connection_manager.worker_job_queues[self.command_handler].popleft()
        self.assertEqual(current_job.handle, fake_job['job_handle'])
        self.assertEqual(current_job.task, fake_job['task'])
        self.assertEqual(current_job.unique, fake_job['unique'])
        self.assertEqual(current_job.data, fake_job['data'])

        # At the end of recv_command(GEARMAN_COMMAND_JOB_ASSIGN_UNIQ)
        self.assert_job_lock(is_locked=False)
        self.assert_sent_command(GEARMAN_COMMAND_PRE_SLEEP)

    def assert_awaiting_job(self):
        self.assert_sent_command(GEARMAN_COMMAND_GRAB_JOB_UNIQ)
        self.assert_no_pending_commands()

    def assert_job_lock(self, is_locked):
        expected_value = (is_locked and self.command_handler) or None
        self.assertEqual(self.connection_manager.command_handler_holding_job_lock, expected_value)

if __name__ == '__main__':
    unittest.main()

