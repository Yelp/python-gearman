# TODO: Merge the binary and server command flows

import socket, struct, errno, logging

from gearman.errors import ConnectionError, ProtocolError
from gearman.constants import DEFAULT_GEARMAN_PORT
from gearman.protocol import GEARMAN_COMMAND_TO_NAME, GEARMAN_PARAMS_FOR_COMMAND, GEARMAN_COMMAND_TEXT_COMMAND, NULL_CHAR, \
    get_command_name, pack_binary_command, parse_binary_command, parse_text_command, pack_text_command

gearman_logger = logging.getLogger('gearman.connection')

class GearmanConnection(object):
    """A connection between a client/worker and a server.  Can be used to reconnect (unlike a socket)
    
    Wraps a socket and provides the following functionality:
       Full read/write methods for Gearman BINARY commands and responses
       Read and Write for Gearman SERVER commands
       No read/write convenience methods for Gearman SERVER responses, use 
    
    Represents a BLOCKING or NON-BLOCKING socket depending on the blocking_timeout as passed in __init__
    """ 
    
    def __init__(self, hostname, port=DEFAULT_GEARMAN_PORT, blocking_timeout=0.0):
        port = port or DEFAULT_GEARMAN_PORT
        self.gearman_host = hostname
        self.gearman_port = port

        # If blocking_timeout == 0.0, this connection becomes a NON-blocking socket
        self.blocking_timeout = blocking_timeout

        self._reset_connection()

    def _reset_connection(self):
        self.gearman_socket = None
        self._is_connected = False
        self._reset_queues()

    def _reset_queues(self):
        self._input_buffer = ''
        self._output_buffer = ''

    def fileno(self):
        """Implements fileno() for use with select.select()"""
        assert self.gearman_socket, 'No socket set'
        return self.gearman_socket.fileno()

    def get_address(self):
        """Returns the host and port"""
        return (self.gearman_host, self.gearman_port)

    def writable(self):
        return self._is_connected and self._output_buffer

    def readable(self):
        return self._is_connected

    def connect(self):
        """Connect to the server. Raise ConnectionError if connection fails."""
        if self._is_connected:
            return

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_socket.connect((self.gearman_host, self.gearman_port))
        except (socket.error, socket.timeout), exc:
            self._reset_connection()
            raise ConnectionError(str(exc))

        self.bind_socket(client_socket)

    def bind_socket(self, current_socket):
        if self.blocking_timeout != 0.0:
            current_socket.setblocking(1)
        else:
            current_socket.setblocking(0)
    
        current_socket.settimeout(self.blocking_timeout)
        current_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, struct.pack('L', 1))

        self._is_connected = True
        self._reset_queues()
        self.gearman_socket = current_socket

    def is_connected(self):
        return self._is_connected

    def recv_command(self, is_response=True):
        received_commands = self.recv_command_list(is_response=is_response)
        assert len(received_commands) == 1, 'Received multiple commands when only expecting 1: %r' % received_commands

        return received_commands[0]

    def recv_command_list(self, is_response=True):
        # Trigger a read on the socket
        self.recv_data_to_buffer()

        given_buffer = self.recv_data_from_buffer()
        received_commands, bytes_read = self.convert_data_to_commands(given_buffer, is_response=is_response)

        # Pull these bytes off the buffer
        self.recv_binary_string(bytes_to_read=bytes_read)

        return received_commands

    def recv_binary_string(self, bytes_to_read=4096):
        """Returns AND removes the read content from the input buffer"""
        data_read = self._input_buffer[:bytes_to_read]
        self._input_buffer = self._input_buffer[bytes_to_read:]

        return data_read

    def recv_data_from_buffer(self):
        """Reads from the buffer but doesn't advance the buffer"""
        return self._input_buffer

    def recv_data_to_buffer(self, size=4096):
        """
        Returns a list of commands: [(cmd_name, cmd_args), ...]
        Raises ConnectionError if the connection dies.
            or ProtocolError if parsing of a command fails.
        """
        assert self._is_connected and self.gearman_socket, "Cannot receive data if we don't have a connection"

        received_commands = []
        recv_buffer = ''
        try:
            recv_buffer = self.gearman_socket.recv(size)
        except socket.error, exc:
            if exc.args[0] == errno.EWOULDBLOCK:
                return ''
            if exc.args[0] == errno.ECONNRESET:
                self.gearman_socket.close()
                raise ConnectionError('connection reset died')
            else:
                raise

        self._input_buffer += recv_buffer
        return len(self._input_buffer)

    def convert_data_to_commands(self, given_buffer, is_response):
        """Takes a binary string and converts it into a list of commands we read.

        is_response=True/False sets the expected magic type (\x00REQ or \x00RES)
        """
        # Read the input buffer and store all our parsed commands in a queue
        bytes_read = 0
        bytes_total = len(given_buffer)

        received_commands = []

        # While we still have bytes to read...
        while bytes_read < bytes_total:
            command_buffer = given_buffer[bytes_read:]

            cmd_type, cmd_args, cmd_len = self.parse_command_from_buffer(command_buffer, is_response=is_response)
            if not cmd_len:
                break

            gearman_logger.debug('%s - Recv - %s - %r', hex(id(self)), get_command_name(cmd_type), cmd_args)

            bytes_read += cmd_len
            received_commands.append((cmd_type, cmd_args))

        return received_commands, bytes_read

    def parse_command_from_buffer(self, given_buffer, is_response=True):
        """Conditionally parse a binary command or a text based server command"""
        if given_buffer[0] == NULL_CHAR:
            return parse_binary_command(given_buffer, is_response=is_response)
        else:
            return parse_text_command(given_buffer)

    def send_command(self, cmd_type, cmd_args=None, is_response=False):
        """Buffered method, queues and sends a single Gearman command"""
        cmd_args = cmd_args or {}
        send_cmd_tuple = (cmd_type, cmd_args, is_response)
        return self.send_command_list([send_cmd_tuple])

    def send_command_list(self, cmd_list):
        """Buffered method, queues and sends a list of Gearman commands"""
        byte_string_to_send = self.convert_commands_to_data(cmd_list)
        self.send_data_to_buffer(byte_string_to_send)

        self.send_data_from_buffer()

    def send_binary_string(self, given_buffer):
        """Buffered method, queues and sends a binary string"""
        self.send_data_to_buffer(given_buffer)
        return self.send_data_from_buffer()

    def send_data_to_buffer(self, given_buffer):
        """Adds data to the outgoing buffer, use self.send_data_from_buffer to write to the socket"""
        self._output_buffer += given_buffer

    def send_data_from_buffer(self):
        """Try to send out some bytes we have stored in our output buffer

        Returns remaining size of the output buffer
        """
        assert self._is_connected and self.gearman_socket, "Cannot receive data if we don't have a connection"

        if not bool(self._output_buffer):
            return 0

        try:
            bytes_sent = self.gearman_socket.send(self._output_buffer)
        except socket.error, exc:
            if exc.args[0] == errno.EWOULDBLOCK:
                return len(self._output_buffer)

            self.close()
            raise ConnectionError(str(exc))

        self._output_buffer = self._output_buffer[bytes_sent:]
        return len(self._output_buffer)

    def convert_commands_to_data(self, cmd_list):
        """Takes a list of tuples (cmd_type, cmd_args, is_response) and converts these commands to a binary string"""
        output_buffer = ''
        for cmd_type, cmd_args, is_response in cmd_list:
            output_buffer += self.pack_command_for_buffer(cmd_type, cmd_args, is_response=is_response)

            gearman_logger.debug('%s - Send - %s - %r', hex(id(self)), get_command_name(cmd_type), cmd_args)

        return output_buffer

    def pack_command_for_buffer(self, cmd_type, cmd_args, is_response):
        """Converts a requested gearman command to its raw binary packet"""
        if cmd_type not in GEARMAN_PARAMS_FOR_COMMAND:
            raise ProtocolError('Unknown command: %r' % cmd_type)
        elif cmd_type == GEARMAN_COMMAND_TEXT_COMMAND:
            return pack_text_command(cmd_type, cmd_args)
        else:
            return pack_binary_command(cmd_type, cmd_args, is_response)

    def close(self):
        """Shutdown our existing socket and reset all of our connection data"""
        try:
            self.gearman_socket.close()
        except Exception:
            pass

        self._reset_connection()

    def __repr__(self):
        return ('<GearmanConnection %s:%d connected=%s>' %
            (self.gearman_host, self.gearman_port, self._is_connected))
