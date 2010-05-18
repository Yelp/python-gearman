import collections
import errno
import logging
import socket
import struct

from gearman.errors import ConnectionError, ProtocolError
from gearman.constants import DEFAULT_GEARMAN_PORT
from gearman.protocol import GEARMAN_PARAMS_FOR_COMMAND, GEARMAN_COMMAND_TEXT_COMMAND, NULL_CHAR, \
    get_command_name, pack_binary_command, parse_binary_command, parse_text_command, pack_text_command

gearman_logger = logging.getLogger('gearman._connection')

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

        self._is_client_side = None
        self._is_server_side = None

        # Reset all our queues
        self._incoming_buffer = ''
        self._outgoing_buffer = ''

        self._incoming_commands = collections.deque()
        self._outgoing_commands = collections.deque()

    def fileno(self):
        """Implements fileno() for use with select.select()"""
        assert self.gearman_socket, 'No socket set'
        return self.gearman_socket.fileno()

    def get_address(self):
        """Returns the host and port"""
        return (self.gearman_host, self.gearman_port)

    def writable(self):
        return self._is_connected and bool(self._outgoing_commands or self._outgoing_buffer)

    def readable(self):
        return self._is_connected

    def connect(self):
        """Connect to the server. Raise ConnectionError if connection fails."""
        if self._is_connected:
            return

        self._reset_connection()

        self.bind_client_socket()

        self._is_client_side = True
        self._is_server_side = False

    def bind_client_socket(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_socket.connect((self.gearman_host, self.gearman_port))
        except socket.error, exc:
            raise ConnectionError(str(exc))

        self.bind_socket(client_socket)

    def bind_socket(self, current_socket):
        if self._is_connected:
            raise ConnectionError("Attempted to bind a socket on a connection that's already connected")

        if self.blocking_timeout != 0.0:
            current_socket.setblocking(1)
        else:
            current_socket.setblocking(0)

        current_socket.settimeout(self.blocking_timeout)
        current_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, struct.pack('L', 1))
        self.gearman_socket = current_socket

        self._is_connected = True

    def is_connected(self):
        return self._is_connected

    def read_command(self):
        """Reads data from the current command queue"""
        if not self._incoming_commands:
            return None

        return self._incoming_commands.popleft()

    def read_commands_from_buffer(self):
        """Reads data from buffer --> command_queue"""
        received_commands = 0
        while True:
            cmd_type, cmd_args, cmd_len = self._unpack_command(self._incoming_buffer)
            if not cmd_len:
                break

            received_commands += 1

            # Store our command on the command queue
            # Move the self._incoming_buffer forward by the number of bytes we just read
            self._incoming_commands.append((cmd_type, cmd_args))
            self._incoming_buffer = self._incoming_buffer[cmd_len:]

        return received_commands

    def read_data_from_socket(self, bytes_to_read=4096):
        """Reads data from socket --> buffer"""
        assert self._is_connected and self.gearman_socket, "Cannot receive data if we don't have a connection"

        recv_buffer = ''
        try:
            recv_buffer = self.gearman_socket.recv(bytes_to_read)
        except socket.error, exc:
            if exc.args[0] == errno.ECONNRESET:
                raise ConnectionError('connection reset died')

            # We should never get here... Gearman API users should never get socket.error's
            # If you run into this, please forward along the error to the __author__
            raise

        if len(recv_buffer) == 0:
            raise ConnectionError('remote disconnected')

        self._incoming_buffer += recv_buffer
        return len(self._incoming_buffer)

    def _unpack_command(self, given_buffer):
        """Conditionally parse a binary command or a text based server command"""
        assert self._is_client_side is not None, "Ambiguous connection state"

        if not given_buffer:
            cmd_type = None
            cmd_args = None
            cmd_len = 0
        elif given_buffer[0] == NULL_CHAR:
            # We'll be expecting a response if we know we're a client side command
            is_response = bool(self._is_client_side)
            cmd_type, cmd_args, cmd_len = parse_binary_command(given_buffer, is_response=is_response)
        else:
            cmd_type, cmd_args, cmd_len = parse_text_command(given_buffer)

        if cmd_type is not None:
            gearman_logger.debug('%s - Recv - %s - %r', hex(id(self)), get_command_name(cmd_type), cmd_args)

        return cmd_type, cmd_args, cmd_len

    def send_command(self, cmd_type, cmd_args=None):
        """Buffered method, queues and sends a single Gearman command"""
        cmd_args = cmd_args or {}
        cmd_tuple = (cmd_type, cmd_args)
        self._outgoing_commands.append(cmd_tuple)

    def send_commands_to_buffer(self):
        """Converts commands to data"""
        if not self._outgoing_commands:
            return

        packed_data = [self._outgoing_buffer]
        while self._outgoing_commands:
            cmd_type, cmd_args = self._outgoing_commands.popleft()
            packed_command = self._pack_command(cmd_type, cmd_args)
            packed_data.append(packed_command)

        self._outgoing_buffer = ''.join(packed_data)

    def send_data_to_socket(self):
        """Try to send out some bytes we have stored in our output buffer

        Returns remaining size of the output buffer
        """
        assert self._is_connected and self.gearman_socket, "Cannot receive data if we don't have a connection"

        if not self._outgoing_buffer:
            return 0

        try:
            bytes_sent = self.gearman_socket.send(self._outgoing_buffer)
        except socket.error, exc:
            if exc.args[0] == errno.EWOULDBLOCK:
                return len(self._outgoing_buffer)
            elif exc.args[0] == errno.ECONNRESET:
                raise ConnectionError('connection reset died')
            # We should never get here... Gearman API users should never get socket.error's
            # If you run into this, please forward along the error to the __author__
            raise

        if bytes_sent == 0:
            raise ConnectionError('remote disconnected')

        self._outgoing_buffer = self._outgoing_buffer[bytes_sent:]
        return len(self._outgoing_buffer)

    def _pack_command(self, cmd_type, cmd_args):
        """Converts a requested gearman command to its raw binary packet"""
        if cmd_type not in GEARMAN_PARAMS_FOR_COMMAND:
            raise ProtocolError('Unknown command: %r' % get_command_name(cmd_type))

        gearman_logger.debug('%s - Send - %s - %r', hex(id(self)), get_command_name(cmd_type), cmd_args)
        if cmd_type == GEARMAN_COMMAND_TEXT_COMMAND:
            return pack_text_command(cmd_type, cmd_args)
        else:
            # We'll be sending a response if we know we're a server side command
            is_response = bool(self._is_server_side)
            return pack_binary_command(cmd_type, cmd_args, is_response)

    def close(self):
        """Shutdown our existing socket and reset all of our connection data"""
        try:
            self.gearman_socket.close()
        except socket.error:
            pass

        self._reset_connection()

    def __repr__(self):
        return ('<GearmanConnection %s:%d connected=%s>' %
            (self.gearman_host, self.gearman_port, self._is_connected))
