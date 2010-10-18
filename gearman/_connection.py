import errno
import socket
import struct
import time


from gearman.errors import ConnectionError

ERRNO_DISCONNECTED = -1

STATE_DISCONNECTED = 'disconnected'
STATE_CONNECTING = 'connecting'
STATE_CONNECTED = 'connected'

class Connection(object):
    """Provide a nice wrapper around an asynchronous socket:

    * Reconnect-able with a given host (unlike a socket)
    * Python-buffered socket for use in asynchronous applications
    """
    reconnect_cooldown_seconds = 1.0
    bytes_to_read = 4096

    def __init__(self, host=None, port=None):
        self._host = host
        self._port = port
        self._socket = None

        if host is None:
            raise self._throw_exception(message='no host')

        if port is None:
            raise self._throw_exception(message='no port')

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
        # If we're already in the process of connecting OR already connected, error out
        if not self.disconnected:
            self._throw_exception(message='invalid connection state')

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

    def send(self, data_array):
        """Returns the data sent to this buffer.  Akin to socket.send(bufsize)"""
        self._outgoing_buffer += data_array
        return len(data_array)

    ########################################################
    ##### Public methods - checking connection state #######
    ########################################################
    @property
    def connected(self):
        return bool(self._state == STATE_CONNECTED)

    @property
    def connecting(self):
        return bool(self._state == STATE_CONNECTING)

    @property
    def disconnected(self):
        return bool(self._state == STATE_DISCONNECTED)

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
        if not self.connected:
            self._throw_exception(message='invalid connection state')

        self._recv_data()

    def handle_write(self):
        if self.disconnected:
            self._throw_exception(message='invalid connection state')
        elif self.connected:
            # Transfer command from command queue -> buffer
            self._send_data()
        else:
            # Check our socket to see what error number we have - See "man connect"
            connect_errno = self._socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            self._update_state(connect_errno)

            if self.connected:
                self.handle_connect()

    def handle_connect(self):
        pass

    def handle_disconnect(self):
        pass

    ########################################################
    ############### Private support methods  ###############
    ########################################################
    def _create_socket(self):
        current_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        current_socket.setblocking(0)
        current_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, struct.pack('L', 1))
        return current_socket

    def _update_state(self, error_number):
        """Update our connection state based on 'errno' """
        if error_number == 0:
            self._state = STATE_CONNECTED
        elif error_number == errno.EINPROGRESS:
            self._state = STATE_CONNECTING
        else:
            self._state = STATE_DISCONNECTED
            self._reset_state()

    def _reset_state(self):
        self._reconnect_time = 0.0

        # Reset all our raw data buffers
        self._incoming_buffer = ''
        self._outgoing_buffer = ''

    def _recv_data(self):
        """Read data from socket -> buffer, returns read # of bytes"""
        if not self.connected:
            self._throw_exception(message='invalid connection state')

        recv_buffer = ''
        try:
            recv_buffer = self._socket.recv(self.bytes_to_read)
        except socket.error, socket_exception:
            self._throw_exception(exception=socket_exception)

        if len(recv_buffer) == 0:
            self.handle_disconnect()

        self._incoming_buffer += recv_buffer
        return len(self._incoming_buffer)

    def _send_data(self):
        """Send data from buffer -> socket

        Returns remaining size of the output buffer
        """
        if not self.connected:
            self._throw_exception(message='invalid connection state')

        if not self._outgoing_buffer:
            return 0

        try:
            bytes_sent = self._socket.send(self._outgoing_buffer)
        except socket.error, socket_exception:
            self._throw_exception(exception=socket_exception)

        if bytes_sent == 0:
            self.handle_disconnect()

        self._outgoing_buffer = self._outgoing_buffer[bytes_sent:]
        return len(self._outgoing_buffer)

    def _throw_exception(self, message=None, exception=None):
        self._update_state(ERRNO_DISCONNECTED)

        if exception:
            message = repr(exception)

        rewritten_message = "<%s:%d> %s" % (self._host, self._port, message)
        raise ConnectionError(rewritten_message)

    def __repr__(self):
        return ('<Connection %s:%d state=%s>' % (self._host, self._port, self._state))
