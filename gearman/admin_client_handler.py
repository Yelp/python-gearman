import collections
import logging

from gearman.command_handler import GearmanCommandHandler
from gearman.errors import ProtocolError, InvalidAdminClientState
from gearman.protocol import GEARMAN_COMMAND_ECHO_REQ, GEARMAN_COMMAND_TEXT_COMMAND, \
    GEARMAN_SERVER_COMMAND_STATUS, GEARMAN_SERVER_COMMAND_VERSION, \
    GEARMAN_SERVER_COMMAND_WORKERS, GEARMAN_SERVER_COMMAND_MAXQUEUE, GEARMAN_SERVER_COMMAND_SHUTDOWN

gearman_logger = logging.getLogger(__name__)

EXPECTED_GEARMAN_SERVER_COMMANDS = set([GEARMAN_SERVER_COMMAND_STATUS, GEARMAN_SERVER_COMMAND_VERSION, \
    GEARMAN_SERVER_COMMAND_WORKERS, GEARMAN_SERVER_COMMAND_MAXQUEUE, GEARMAN_SERVER_COMMAND_SHUTDOWN])

class GearmanAdminClientCommandHandler(GearmanCommandHandler):
    """Special GEARMAN_COMMAND_TEXT_COMMAND command handler that'll parse text responses from the server"""
    STATUS_FIELDS = 4
    WORKERS_FIELDS = 4

    def __init__(self, connection_manager=None):
        super(GearmanAdminClientCommandHandler, self).__init__(connection_manager=connection_manager)
        self._sent_commands = collections.deque()
        self._recv_responses = collections.deque()

        self._status_response = []
        self._workers_response = []

    #######################################################################
    ##### Public interface methods to be called by GearmanAdminClient #####
    #######################################################################

    @property
    def response_ready(self):
        return bool(self._recv_responses)

    def pop_response(self):
        if not self._sent_commands or not self._recv_responses:
            raise InvalidAdminClientState('Attempted to pop a response for a command that is not ready')

        sent_command = self._sent_commands.popleft()
        recv_response = self._recv_responses.popleft()
        return sent_command, recv_response

    def send_text_command(self, command_line):
        """Send our administrative text command"""
        expected_server_command = None
        for server_command in EXPECTED_GEARMAN_SERVER_COMMANDS:
            if command_line.startswith(server_command):
                expected_server_command = server_command
                break

        if not expected_server_command:
            raise ProtocolError('Attempted to send an unknown server command: %r' % command_line)

        self._sent_commands.append(expected_server_command)

        output_text = '%s\n' % command_line
        self.send_command(GEARMAN_COMMAND_TEXT_COMMAND, raw_text=output_text)

    def send_echo_request(self, echo_string):
        """Send our administrative text command"""
        self._sent_commands.append(GEARMAN_COMMAND_ECHO_REQ)

        self.send_command(GEARMAN_COMMAND_ECHO_REQ, data=echo_string)

    ###########################################################
    ### Callbacks when we receive a command from the server ###
    ###########################################################

    def recv_echo_res(self, data):
        self._recv_responses.append(data)
        return False

    def recv_text_command(self, raw_text):
        """Catch GEARMAN_COMMAND_TEXT_COMMAND's and forward them onto their respective recv_server_* callbacks"""
        if not self._sent_commands:
            raise InvalidAdminClientState('Received an unexpected server response')

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

    def recv_server_status(self, raw_text):
        """Slowly assemble a server status message line by line"""
        # If we received a '.', we've finished parsing this status message
        # Pack up our output and reset our response queue
        if raw_text == '.':
            output_response = tuple(self._status_response)
            self._recv_responses.append(output_response)
            self._status_response = []
            return False

        # If we didn't get a final response, split our line and interpret all the data
        split_tokens = raw_text.split('\t')
        if len(split_tokens) != self.STATUS_FIELDS:
            raise ProtocolError('Received %d tokens, expected %d tokens: %r' % (len(split_tokens), self.STATUS_FIELDS, split_tokens))

        # Label our fields and make the results Python friendly
        task, queued_count, running_count, worker_count = split_tokens

        status_dict = {}
        status_dict['task'] = task
        status_dict['queued'] = int(queued_count)
        status_dict['running'] = int(running_count)
        status_dict['workers'] = int(worker_count)
        self._status_response.append(status_dict)
        return True

    def recv_server_version(self, raw_text):
        """Version response is a simple passthrough"""
        self._recv_responses.append(raw_text)
        return False

    def recv_server_workers(self, raw_text):
        """Slowly assemble a server workers message line by line"""
        # If we received a '.', we've finished parsing this workers message
        # Pack up our output and reset our response queue
        if raw_text == '.':
            output_response = tuple(self._workers_response)
            self._recv_responses.append(output_response)
            self._workers_response = []
            return False

        split_tokens = raw_text.split(' ')
        if len(split_tokens) < self.WORKERS_FIELDS:
            raise ProtocolError('Received %d tokens, expected >= 4 tokens: %r' % (len(split_tokens), split_tokens))

        if split_tokens[3] != ':':
            raise ProtocolError('Malformed worker response: %r' % (split_tokens, ))

        # Label our fields and make the results Python friendly
        worker_dict = {}
        worker_dict['file_descriptor'] = split_tokens[0]
        worker_dict['ip'] = split_tokens[1]
        worker_dict['client_id'] = split_tokens[2]
        worker_dict['tasks'] = tuple(split_tokens[4:])
        self._workers_response.append(worker_dict)
        return True

    def recv_server_maxqueue(self, raw_text):
        """Maxqueue response is a simple passthrough"""
        if raw_text != 'OK':
            raise ProtocolError("Expected 'OK', received: %s" % raw_text)

        self._recv_responses.append(raw_text)
        return False

    def recv_server_shutdown(self, raw_text):
        """Shutdown response is a simple passthrough"""
        self._recv_responses.append(None)
        return False