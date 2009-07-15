import socket, struct, select, errno, logging
from time import time

from gearman.protocol import DEFAULT_PORT, pack_command, parse_command

log = logging.getLogger("gearman")

class GearmanConnection(object):
    class ConnectionError(Exception):
        pass

    def __init__(self, host, port=DEFAULT_PORT, sock=None, timeout=None, reconnect_timeout=60*2):
        """
        A connection to a Gearman server.
        """
        if not sock:
            self.sock = None
            self.connected = False
        else:
            self.sock = sock
            self.connected = True

        if ':' in host:
            host, port = (host.split(':') + [0])[:2]
            port = int(port) or DEFAULT_PORT
            self.addr = (host, port)
        else:
            port = port or DEFAULT_PORT
            self.addr = (host, port)
        self.hostspec = "%s:%d" % (host, port)
        self.timeout  = timeout
        self.reconnect_timeout = reconnect_timeout

        self.is_dead = False
        self._reset_queues()

    def _reset_queues(self):
        self.in_buffer = ""
        self.out_buffer = ""
        self.waiting_for_handles = []

    def fileno(self):
        return self.sock.fileno()

    def writable(self):
        return self.connected and len(self.out_buffer) != 0

    def readable(self):
        return self.connected

    def connect(self):
        """Connect to the server. Raise ConnectionError if connection fails."""

        if self.connected:
            return

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        try:
            self.sock.connect(self.addr)
        except (socket.error, socket.timeout), exc:
            self.sock = None
            self.is_dead = True
            raise self.ConnectionError(str(exc))

        self._reset_queues()
        self.is_dead = False
        self.connected = True
        self.sock.setblocking(0)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, struct.pack("L", 1))

    def is_dead():
        def fget(self):
            if self._is_dead and time() >= self._retry_at:
                self._is_dead = False
                # try:
                #     self.connect()
                # except self.ConnectionError:
                #     pass
            return self._is_dead
        def fset(self, value):
            self._is_dead = value
            self._retry_at = value and time() + self.reconnect_timeout or None
        return locals()
    is_dead = property(**is_dead())

    def recv(self, size=4096):
        """
        Returns a list of commands: [(cmd_name, cmd_args), ...]
        Raises ConnectionError if the connection dies.
            or ProtocolError if parsing of a command fails.
        """

        assert self.connected

        data = ''
        try:
            data = self.sock.recv(size)
        except socket.error, exc:
            if exc.args[0] == errno.EWOULDBLOCK:
                return
            if exc.args[0] == errno.ECONNRESET:
                data = None
            else:
                raise

        if not data:
            self.mark_dead()
            raise self.ConnectionError("connection died")

        self.in_buffer += data

        commands = []
        while len(self.in_buffer) >= 12:
            func, args, cmd_len = parse_command(self.in_buffer)
            if not cmd_len:
                break

            commands.append((func, args))

            self.in_buffer = buffer(self.in_buffer, cmd_len)
        return commands

    def send(self):
        assert self.connected

        if len(self.out_buffer) == 0:
            return 0

        try:
            nsent = self.sock.send(self.out_buffer)
        except socket.error, exc:
            if exc.args[0] == errno.EWOULDBLOCK:
                return len(self.out_buffer)
            self.mark_dead()
            raise self.ConnectionError(str(exc))

        self.out_buffer = buffer(self.out_buffer, nsent)

        return len(self.out_buffer)

    def send_command(self, name, kwargs={}):
        pkt = pack_command(name, **kwargs)
        self.out_buffer += pkt
        self.send()

    def flush(self, timeout=None): # TODO: handle connection failures
        while self.writable():
            try:
                wr_list = select.select([], [self], [], timeout)[1] # TODO: exc list
            except select.error, exc:
                # Ignore interrupted system call, reraise anything else
                if exc[0] != 4:
                    raise
            if self in wr_list:
                self.send()

    def send_command_blocking(self, cmd_name, cmd_args={}, timeout=None):
        self.send_command(cmd_name, cmd_args)
        self.flush(timeout)

    def recv_blocking(self, timeout=None):
        if not hasattr(self, '_command_queue'):
            self._command_queue = []

        if timeout:
            end_time = time() + timeout

        while not self._command_queue:
            time_left = timeout and end_time - time() or 0.5

            try:
                rd_list, wr_list, ex_list = select.select([self], self.writable() and [self] or [], [self], time_left)
            except select.error, exc:
                # Ignore interrupted system call, reraise anything else
                if exc[0] != 4:
                    raise
                rd_list = wr_list = ex_list = []

            if self in ex_list:
                self.mark_dead()
                raise self.ConnectionError("connection died")

            if self in rd_list:
                for cmd in self.recv():
                    self._command_queue.insert(0, cmd)

            if self in wr_list:
                self.send()

            if time_left <= 0:
                break

        if self._command_queue:
            return self._command_queue.pop()
        return None

    def close(self):
        self.connected = False
        try:
            self.sock.close()
        except:
            pass
        self.sock = None

    def mark_dead(self):
        self.close()
        self.is_dead = True

    def __repr__(self):
        return ("<GearmanConnection %s:%d connected=%s dead=%s>" %
            (self.addr[0], self.addr[1], self.connected, self.is_dead))
