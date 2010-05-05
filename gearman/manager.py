import collections
import logging

from gearman._client_base import GearmanClientBase, GearmanConnectionHandler
from gearman.errors import ProtocolError, InvalidManagerState
from gearman.protocol import GEARMAN_COMMAND_TEXT_COMMAND, \
    GEARMAN_SERVER_COMMAND_STATUS, GEARMAN_SERVER_COMMAND_VERSION, GEARMAN_SERVER_COMMAND_WORKERS, GEARMAN_SERVER_COMMAND_MAXQUEUE, GEARMAN_SERVER_COMMAND_SHUTDOWN

gearman_logger = logging.getLogger('gearman.manager')

EXPECTED_GEARMAN_SERVER_COMMANDS = set([GEARMAN_SERVER_COMMAND_STATUS, GEARMAN_SERVER_COMMAND_VERSION, GEARMAN_SERVER_COMMAND_WORKERS, GEARMAN_SERVER_COMMAND_MAXQUEUE, GEARMAN_SERVER_COMMAND_SHUTDOWN])

class GearmanManager(GearmanClientBase):
    """GearmanWorkers manage connections and ConnectionHandlers

    This is the public facing gearman interface that most users should be instantiating
    All I/O will be handled by the GearmanWorker
    All state machine operations are handled on the ConnectionHandler
    """
    def __init__(self, **kwargs):
        host_list = kwargs.get('host_list', [])
        assert len(host_list) == 1, 'Only expected a single host'

        # By default we should have non-blocking sockets for a GearmanWorker
        kwargs.setdefault('blocking_timeout', 5.0)
        kwargs.setdefault('gearman_connection_handler_class', GearmanManagerConnectionHandler)
        super(GearmanManager, self).__init__(**kwargs)

        self.blocking_timeout = kwargs.get('blocking_timeout')

        self.current_connection = self.connection_list[0]
        self.current_handler = self.connection_handlers[self.current_connection]

        self.current_connection.connect()

    def maxqueue(self, function_name, max_size):
        self.current_handler.send_text_command('%s %s %s' % (GEARMAN_SERVER_COMMAND_MAXQUEUE, function_name, max_size))
        cmd_resp = self.poll_connections_until_server_responds(GEARMAN_SERVER_COMMAND_MAXQUEUE)

        return cmd_resp

    def status(self):
        self.current_handler.send_text_command(GEARMAN_SERVER_COMMAND_STATUS)
        cmd_resp = self.poll_connections_until_server_responds(GEARMAN_SERVER_COMMAND_STATUS)

        return cmd_resp

    def shutdown(self, graceful=True):
        actual_command = GEARMAN_SERVER_COMMAND_SHUTDOWN
        if graceful:
            actual_command += ' graceful'

        self.current_handler.send_text_command(actual_command)
        cmd_resp = self.poll_connections_until_server_responds(GEARMAN_SERVER_COMMAND_SHUTDOWN)

        return cmd_resp

    def version(self):
        self.current_handler.send_text_command(GEARMAN_SERVER_COMMAND_VERSION)
        cmd_resp = self.poll_connections_until_server_responds(GEARMAN_SERVER_COMMAND_VERSION)

        return cmd_resp

    def workers(self):
        self.current_handler.send_text_command(GEARMAN_SERVER_COMMAND_WORKERS)
        cmd_resp = self.poll_connections_until_server_responds(GEARMAN_SERVER_COMMAND_WORKERS)

        return cmd_resp

    def poll_connections_until_server_responds(self, expected_type):
        def continue_while_no_response(self, any_activity):
            return (not self.current_handler.has_response())

        self.poll_connections_until_stopped([self.current_connection], continue_while_no_response, timeout=self.blocking_timeout)
        cmd_type, cmd_resp = self.current_handler.pop_response()

        if cmd_type != expected_type:
            raise InvalidManagerState('Received an unexpected response... got command %r, expecting command %r' % (cmd_type, expected_type))

        return cmd_resp

class GearmanManagerConnectionHandler(GearmanConnectionHandler):
    """GearmanManager state machine on a per connection basis"""
    def __init__(self, *largs, **kwargs):
        super(GearmanManagerConnectionHandler, self).__init__(*largs, **kwargs)

        self._sent_commands = collections.deque()
        self._recv_responses = collections.deque()

        self._status_fields = ('function_name', 'queued', 'running', 'workers')
        self._status_response = []

        self._workers_response = []

    def has_response(self):
        return bool(self._recv_responses)

    def pop_response(self):
        if not self._sent_commands or not self._recv_responses:
            raise InvalidManagerState('Attempted to pop a response for a command that is not ready')

        sent_command = self._sent_commands.popleft()
        recv_response = self._recv_responses.popleft()
        return sent_command, recv_response

    # Send Gearman commands related to jobs
    def send_text_command(self, command_line):
        expected_server_command = None
        for server_command in EXPECTED_GEARMAN_SERVER_COMMANDS:
            if command_line.startswith(server_command):
                expected_server_command = server_command
                break

        if not expected_server_command:
            raise ProtocolError('Attempted to send an unknown server command: %s' % expected_server_command)

        self._sent_commands.append(expected_server_command)

        output_text = '%s\n' % command_line
        self.send_command(GEARMAN_COMMAND_TEXT_COMMAND, raw_text=output_text)

    def recv_error(self, error_code, error_text):
        gearman_logger.error('Error from server: %s: %s' % (error_code, error_text))
        self.client_base.handle_error(self.gearman_connection)

        return False

    def recv_text_command(self, raw_text):
        if not self._sent_commands:
            raise InvalidManagerState('Received an unexpected server response')

        # Peek at the first command
        cmd_type = self._sent_commands[0]
        recv_server_command_function_name = 'recv_server_%s' % cmd_type

        cmd_callback = getattr(self, recv_server_command_function_name, None)
        if not cmd_callback:
            gearman_logger.error('Could not handle command: %r - %r' % (cmd_type, raw_text))
            raise ValueError('Could not handle command: %r - %r' % (cmd_type, raw_text))

        # This must match the parameter names as defined in the command handler
        completed_work = cmd_callback(raw_text)
        return completed_work

    # Begin receiving server messages
    def recv_server_status(self, raw_text):
        """Parse a multi-line status message line by line"""
        if raw_text == '.':
            output_response = tuple(self._status_response)
            self._recv_responses.append(output_response)
            self._status_response = []
            return False

        split_tokens = raw_text.split('\t')
        if len(split_tokens) != len(self._status_fields):
            raise ProtocolError('Received %d tokens, expected %d tokens: %r' % (len(split_tokens), len(self._status_fields), split_tokens))

        # Label our fields
        status_dict = dict((field_name, field_value) for field_name, field_value in zip(self._status_fields, split_tokens))
        self._status_response.append(status_dict)
        return True

    def recv_server_version(self, raw_text):
        self._recv_responses.append(raw_text)
        return False

    def recv_server_workers(self, raw_text):
        if raw_text == '.':
            output_response = tuple(self._workers_response)
            self._recv_responses.append(output_response)
            self._workers_response = []
            return False

        split_tokens = raw_text.split(' ')
        if len(split_tokens) < 4:
            raise ProtocolError('Received %d tokens, expected >= 4 tokens: %r' % (len(split_tokens), split_tokens))

        if split_tokens[3] != ':':
            raise ProtocolError('Malformed worker response: %r' % (split_tokens, ))

        worker_dict = {}
        worker_dict['file_descriptor'] = split_tokens[0]
        worker_dict['ip'] = split_tokens[1]
        worker_dict['client_id'] = split_tokens[2]
        worker_dict['function_names'] = split_tokens[4:]

        # Label our fields
        self._workers_response.append(worker_dict)
        return True

    def recv_server_maxqueue(self, raw_text):
        if raw_text != 'OK':
            raise ProtocolError("Expected 'OK', received: %s" % raw_text)

        self._recv_responses.append(raw_text)
        return True

    def recv_server_shutdown(self, raw_text):
        self._recv_responses.append(None)
        return False
