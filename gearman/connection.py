import collections
import errno
import logging
import socket
import struct
import time

from gearman.errors import ConnectionError, ProtocolError, ServerUnavailable
from gearman.constants import DEFAULT_PORT, _DEBUG_MODE_
from gearman.protocol import GEARMAN_PARAMS_FOR_COMMAND, GEARMAN_COMMAND_TEXT_COMMAND, NULL_CHAR, \
    get_command_name, pack_binary_command, parse_binary_command, parse_text_command, pack_text_command

gearman_logger = logging.getLogger(__name__)

ERRNO_DISCONNECTED = -1

STATE_DISCONNECTED = 'disconnected'
STATE_CONNECTING = 'connecting'
STATE_CONNECTED = 'connected'

class GearmanConnection(object):
    """Provide a nice wrapper around an asynchronous socket:

    * Reconnect-able with a given host (unlike a socket)
    * Python-buffered socket for use in asynchronous applications
    """
    reconnect_cooldown_seconds = 1.0
    bytes_to_read = 4096

    def __init__(self, host=None, port=DEFAULT_PORT):
        port = port or DEFAULT_PORT
        if host is None:
            raise ServerUnavailable("No host specified")

        self._host = host
        self._port = port

        self._socket = None

        self._update_state(ERRNO_DISCONNECTED)

    ########################################################
    ##### Public methods to masquerade like a socket #######
    ########################################################
    def accept(self):
        raise NotImplementedError

    def bind(self, socket_address):
        raise NotImplementedError

    def close(self):
        """Shutdown our existing socket and reset all of our connection data"""
        if not self._socket:
            self._throw_exception(message='no socket')

        try:
           self._socket.close()
        except socket.error:
            pass

        self._reset_state()

    def connect(self):
        """Connect to the server. Raise ConnectionError if connection fails."""
        if self.connecting:
            self._throw_exception(message='connection in progress')
        elif self.connected:
            self._throw_exception(message='connection already established')

        current_time = time.time()
        if current_time < self._reconnect_time:
            self._throw_exception(message='attempted to reconnect too quickly')

        self._reconnect_time = current_time + self.reconnect_cooldown_seconds

        # Attempt to do an asynchronous connect
        self._socket = self._create_socket()
        try:
            socket_address = self.getpeername()
            connect_errno = self._socket.connect_ex(socket_address)
        except socket.error, socket_exception:
            self._throw_exception(exception=socket_exception)

        self._update_state(connect_errno)

    def fileno(self):
        """Implements fileno() for use with select.select()"""
        if not self._socket:
            self._throw_exception(message='no socket')

        return self._socket.fileno()

    def getpeername(self):
        """Returns the host and port as if this where a AF_INET socket"""
        return (self._host, self._port)

    def peek(self, bufsize=None):
        """Reads data seen on this socket WITHOUT the buffer advancing.  Akin to socket.recv(bufsize, socket.MSG_PEEK)"""
        if bufsize is None:
            return self._incoming_buffer
        else:
            return self._incoming_buffer[:bufsize]

    def recv(self, bufsize=None):
        """Reads data seen on this socket WITH the buffer advancing.  Akin to socket.recv(bufsize)"""
        recv_buffer = self.peek(bufsize)

        recv_bytes = len(recv_buffer)
        if recv_bytes:
            self._incoming_buffer = self._incoming_buffer[recv_bytes:]

        return recv_buffer

    def send(self, string):
        """Returns the data read on this buffer and advances the buffer.  Akin to socket.recv(bufsize)"""
        self._outgoing_buffer += string
        return len(string)

    ########################################################
    ##### Public methods - checking connection state #####
    ########################################################
    @property
    def connected(self):
        return bool(self._state == STATE_CONNECTED)

    @property
    def connecting(self):
        return bool(self._state == STATE_CONNECTING)

    def readable(self):
        """Returns True if we might have data to read"""
        return bool(self.connected)

    def writable(self):
        """Returns True if we have data to write"""
        connected_and_pending_writes = self.connected and bool(self._outgoing_buffer)
        connecting_in_progress = self.connecting
        return bool(connected_and_pending_writes or connecting_in_progress)

    def handle_read(self):
        """Reads data from socket --> buffer"""
        if self.connecting:
            self._throw_exception(message='connection in progress')

        self._recv_data()

    def handle_write(self):
        if self.connecting:
            # Check our socket to see what error number we have - See "man connect"
            connect_errno = self._socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)

            self._update_state(connect_errno)
        else:
            # Transfer command from command queue -> buffer
            self._send_data()

    def _recv_data(self):
        """Read data from socket -> buffer"""
        if not self.connected:
            self._throw_exception(message='disconnected')

        recv_buffer = ''
        try:
            recv_buffer = self._socket.recv(self.bytes_to_read)
        except socket.error, socket_exception:
            self._throw_exception(exception=socket_exception)

        if len(recv_buffer) == 0:
            self._throw_exception(message='remote disconnected')

        self._incoming_buffer += recv_buffer
        return len(self._incoming_buffer)

    def _send_data(self):
        """Send data from buffer -> socket

        Returns remaining size of the output buffer
        """
        if not self.connected:
            self._throw_exception(message='disconnected')

        if not self._outgoing_buffer:
            return 0

        try:
            bytes_sent = self._socket.send(self._outgoing_buffer)
        except socket.error, socket_exception:
            self._throw_exception(exception=socket_exception)

        if bytes_sent == 0:
            self._throw_exception(message='remote disconnected')

        self._outgoing_buffer = self._outgoing_buffer[bytes_sent:]
        return len(self._outgoing_buffer)

    ########################################################
    ############### Private support methods  ###############
    ########################################################
    def _create_socket(self):
        current_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        current_socket.setblocking(0)
        current_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, struct.pack('L', 1))
        return current_socket

    def _update_state(self, error_number):
        """Update our connection state based on 'errno'  """
        if error_number == 0:
            self._state = STATE_CONNECTED
        elif error_number == errno.EINPROGRESS:
            self._state = STATE_CONNECTING
        else:
            self._state = STATE_DISCONNECTED
            self._reconnect_time = 0.0

            # Reset all our raw data buffers
            self._incoming_buffer = ''
            self._outgoing_buffer = ''

    def _throw_exception(self, message=None, exception=None):
        self._update_state(ERRNO_DISCONNECTED)

        if exception:
            message = repr(exception)

        rewritten_message = "<%s:%d> %s" % (self._host, self._port, message)
        raise ConnectionError(rewritten_message)

    def __repr__(self):
        return ('<GearmanConnection %s:%d state=%s>' % (self._host, self._port, self._state))
