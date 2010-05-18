import logging

from gearman import util

from gearman._connection_manager import GearmanConnectionManager
from gearman.admin_client_handler import GearmanAdminClientCommandHandler
from gearman.errors import InvalidAdminClientState
from gearman.protocol import GEARMAN_SERVER_COMMAND_STATUS, GEARMAN_SERVER_COMMAND_VERSION, GEARMAN_SERVER_COMMAND_WORKERS, GEARMAN_SERVER_COMMAND_MAXQUEUE, GEARMAN_SERVER_COMMAND_SHUTDOWN

gearman_logger = logging.getLogger('gearman.admin_client')

class GearmanAdminClient(GearmanConnectionManager):
    """GearmanWorkers manage connections and CommandHandlers

    This is the public facing gearman interface that most users should be instantiating
    All I/O will be handled by the GearmanWorker
    All state machine operations are handled on the CommandHandler
    """
    command_handler_class = GearmanAdminClientCommandHandler

    def __init__(self, *largs, **kwargs):
        host_list = kwargs.get('host_list', [])
        is_testing = kwargs.pop('_is_testing_', False)
        if not is_testing:
            assert len(host_list) == 1, 'Only expected a single host'

        kwargs.setdefault('blocking_timeout', 5.0)
        super(GearmanAdminClient, self).__init__(*largs, **kwargs)

        self.blocking_timeout = kwargs.get('blocking_timeout')

        if not is_testing:
	        self.current_connection = util.unlist(self.connection_list)
	        self.current_handler = util.unlist(self.handler_to_connection_map.keys())

	        self.current_connection.connect()
        else:
            self.current_connection = None
            self.current_handler = None

    def send_maxqueue(self, function_name, max_size):
        self.current_handler.send_text_command('%s %s %s' % (GEARMAN_SERVER_COMMAND_MAXQUEUE, function_name, max_size))
        return self.wait_until_server_responds(GEARMAN_SERVER_COMMAND_MAXQUEUE)

    def send_shutdown(self, graceful=True):
        actual_command = GEARMAN_SERVER_COMMAND_SHUTDOWN
        if graceful:
            actual_command += ' graceful'

        self.current_handler.send_text_command(actual_command)
        return self.wait_until_server_responds(GEARMAN_SERVER_COMMAND_SHUTDOWN)

    def get_status(self):
        self.current_handler.send_text_command(GEARMAN_SERVER_COMMAND_STATUS)
        return self.wait_until_server_responds(GEARMAN_SERVER_COMMAND_STATUS)

    def get_version(self):
        self.current_handler.send_text_command(GEARMAN_SERVER_COMMAND_VERSION)
        return self.wait_until_server_responds(GEARMAN_SERVER_COMMAND_VERSION)

    def get_workers(self):
        self.current_handler.send_text_command(GEARMAN_SERVER_COMMAND_WORKERS)
        return self.wait_until_server_responds(GEARMAN_SERVER_COMMAND_WORKERS)

    def wait_until_server_responds(self, expected_type):
        current_handler = self.current_handler
        def continue_while_no_response(any_activity, callback_data):
            return (not current_handler.has_response())

        self.poll_connections_until_stopped([self.current_connection], continue_while_no_response, timeout=self.blocking_timeout)
        cmd_type, cmd_resp = self.current_handler.pop_response()

        if cmd_type != expected_type:
            raise InvalidAdminClientState('Received an unexpected response... got command %r, expecting command %r' % (cmd_type, expected_type))

        return cmd_resp
