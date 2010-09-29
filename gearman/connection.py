import collections
import logging
import socket
import struct
import time

from gearman.errors import ConnectionError, ProtocolError, ServerUnavailable
from gearman.constants import DEFAULT_GEARMAN_PORT, _DEBUG_MODE_
from gearman.protocol import GEARMAN_PARAMS_FOR_COMMAND, GEARMAN_COMMAND_TEXT_COMMAND, NULL_CHAR, \
    get_command_name, pack_binary_command, parse_binary_command, parse_text_command, pack_text_command

gearman_logger = logging.getLogger(__name__)

class GearmanConnection(object):
    """A connection between a client/worker and a server.  Can be used to reconnect (unlike a socket)

    Wraps a socket and provides the following functionality:
        Full read/write methods for Gearman BINARY commands and responses
        Full read/write methods for Gearman SERVER commands and responses (using GEARMAN_COMMAND_TEXT_COMMAND)

        Manages raw data buffers for socket-level operations
        Manages command buffers for gearman-level operations

    All I/O and buffering should be done in this class
    """
    connect_cooldown_seconds = 1.0

    def __init__(self, host=None, port=DEFAULT_GEARMAN_PORT):
        port = port or DEFAULT_GEARMAN_PORT
        self.gearman_host = host
        self.gearman_port = port

        if host is None:
            raise ServerUnavailable("No host specified")

        self._reset_connection()

    def _reset_connection(self):
        """Reset the state of this connection"""
        self.connected = False
        self.gearman_socket = None

        self.allowed_connect_time = 0.0

        self._is_client_side = None
        self._is_server_side = None

        # Reset all our raw data buffers
        self._incoming_buffer = ''
        self._outgoing_buffer = ''

        # Toss all commands we may have sent or received
        self._incoming_commands = collections.deque()
        self._outgoing_commands = collections.deque()

    def fileno(self):
        """Implements fileno() for use with select.select()"""
        if not self.gearman_socket:
            self.throw_exception(message='no socket set')

        return self.gearman_socket.fileno()

    def get_address(self):
        """Returns the host and port"""
        return (self.gearman_host, self.gearman_port)

    def writable(self):
        """Returns True if we have data to write"""
        return self.connected and bool(self._outgoing_commands or self._outgoing_buffer)

    def readable(self):
        """Returns True if we might have data to read"""
        return self.connected

    def connect(self):
        """Connect to the server. Raise ConnectionError if connection fails."""
        if self.connected:
            self.throw_exception(message='connection already established')

        current_time = time.time()
        if current_time < self.allowed_connect_time:
            self.throw_exception(message='attempted to connect before required cooldown')

        self.allowed_connect_time = current_time + self.connect_cooldown_seconds

        self._reset_connection()

        self._create_client_socket()

        self.connected = True
        self._is_client_side = True
        self._is_server_side = False

    def _create_client_socket(self):
        """Creates a client side socket and subsequently binds/configures our socket options"""
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((self.gearman_host, self.gearman_port))
        except socket.error, socket_exception:
            self.throw_exception(exception=socket_exception)

        self.set_socket(client_socket)

    def set_socket(self, current_socket):
        """Setup common options for all Gearman-related sockets"""
        if self.gearman_socket:
            self.throw_exception(message='socket already bound')

        current_socket.setblocking(0)
        current_socket.settimeout(0.0)
        current_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, struct.pack('L', 1))
        self.gearman_socket = current_socket

    def read_command(self):
        """Reads a single command from the command queue"""
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
        if not self.connected:
            self.throw_exception(message='disconnected')

        recv_buffer = ''
        try:
            recv_buffer = self.gearman_socket.recv(bytes_to_read)
        except socket.error, socket_exception:
            self.throw_exception(exception=socket_exception)

        if len(recv_buffer) == 0:
            self.throw_exception(message='remote disconnected')

        self._incoming_buffer += recv_buffer
        return len(self._incoming_buffer)

    def _unpack_command(self, given_buffer):
        """Conditionally unpack a binary command or a text based server command"""
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

        if _DEBUG_MODE_ and cmd_type is not None:
            gearman_logger.debug('%s - Recv - %s - %r', hex(id(self)), get_command_name(cmd_type), cmd_args)

        return cmd_type, cmd_args, cmd_len

    def send_command(self, cmd_type, cmd_args):
        """Adds a single gearman command to the outgoing command queue"""
        self._outgoing_commands.append((cmd_type, cmd_args))

    def send_commands_to_buffer(self):
        """Sends and packs commands -> buffer"""
        if not self._outgoing_commands:
            return

        packed_data = [self._outgoing_buffer]
        while self._outgoing_commands:
            cmd_type, cmd_args = self._outgoing_commands.popleft()
            packed_command = self._pack_command(cmd_type, cmd_args)
            packed_data.append(packed_command)

        self._outgoing_buffer = ''.join(packed_data)

    def send_data_to_socket(self):
        """Send data from buffer -> socket

        Returns remaining size of the output buffer
        """
        if not self.connected:
            self.throw_exception(message='disconnected')

        if not self._outgoing_buffer:
            return 0

        try:
            bytes_sent = self.gearman_socket.send(self._outgoing_buffer)
        except socket.error, socket_exception:
            self.throw_exception(exception=socket_exception)

        if bytes_sent == 0:
            self.throw_exception(message='remote disconnected')

        self._outgoing_buffer = self._outgoing_buffer[bytes_sent:]
        return len(self._outgoing_buffer)

    def _pack_command(self, cmd_type, cmd_args):
        """Converts a command to its raw binary format"""
        if cmd_type not in GEARMAN_PARAMS_FOR_COMMAND:
            raise ProtocolError('Unknown command: %r' % get_command_name(cmd_type))

        if _DEBUG_MODE_:
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
            if self.gearman_socket:
                self.gearman_socket.close()
        except socket.error:
            pass

        self._reset_connection()

    def throw_exception(self, message=None, exception=None):
        # Mark us as disconnected but do NOT call self._reset_connection()
        # Allows catchers of ConnectionError a chance to inspect the last state of this connection
        self.connected = False

        if exception:
            message = repr(exception)

        rewritten_message = "<%s:%d> %s" % (self.gearman_host, self.gearman_port, message)
        raise ConnectionError(rewritten_message)

    def __repr__(self):
        return ('<GearmanConnection %s:%d connected=%s>' %
            (self.gearman_host, self.gearman_port, self.connected))
