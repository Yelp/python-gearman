import socket, struct, select, errno
from time import time

DEFAULT_PORT = 7003

COMMANDS = {
     1: ("can_do", ["func"]),
    23: ("can_do_timeout", ["func", "timeout"]),
     2: ("cant_do", ["func"]),
     3: ("reset_abilities", []),
    22: ("set_client_id", ["client_id"]),
     4: ("pre_sleep", []),

     6: ("noop", []),
     7: ("submit_job", ["func", "uniq", "arg"]),
    21: ("submit_job_high", ["func", "uniq", "arg"]),
    18: ("submit_job_bg", ["func", "uniq", "arg"]),

     8: ("job_created", ["handle"]),
     9: ("grab_job", []),
    10: ("no_job", []),
    11: ("job_assign", ["handle", "func", "arg"]),

    12: ("work_status", ["handle", "numerator", "denominator"]),
    13: ("work_complete", ["handle", "result"]),
    14: ("work_fail", ["handle"]),

    15: ("get_status", ["handle"]),
    20: ("status_res", ["handle", "known", "running", "numerator", "denominator"]),

    16: ("echo_req", ["text"]),
    17: ("echo_res", ["text"]),

    19: ("error", ["err_code", "err_text"]),

    24: ("all_yours", []),
}
# Create a mapping of function name -> id, args
R_COMMANDS = dict((m[0], (mid, m[1])) for mid,m in COMMANDS.iteritems())

class GearmanConnection(object):
    class ConnectionError(Exception): pass
    class ProtocolError(Exception): pass

    def __init__(self, host, port=DEFAULT_PORT, sock=None, timeout=None):
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
        self.sock.settimeout( self.timeout )
        try:
            self.sock.connect( self.addr )
        except (socket.error, socket.timeout), e:
            self.sock = None
            self.is_dead = True
            raise self.ConnectionError(str(e))

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
            self._retry_at = value and time() + 60*2 or None # TODO: should be configurable
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
        except socket.error, e:
            if e.args[0] == errno.EWOULDBLOCK:
                return
            raise
        else:
            if not data:
                self.close()
                self.is_dead = True
                raise self.ConnectionError("connection died")

        self.in_buffer += data

        commands = []
        while len(self.in_buffer) >= 12:
            magic, typ, data_len = struct.unpack("!4sLL", self.in_buffer[0:12])
            if len(self.in_buffer) < 12 + data_len:
                break

            if magic != "\x00RES":
                raise self.ProtocolError("Malformed Magic")

            commands.append(self._parse_command(typ, self.in_buffer[12:12 + data_len]))

            self.in_buffer = buffer(self.in_buffer, 12 + data_len)
        return commands

    def send(self):
        assert self.connected

        if len(self.out_buffer) == 0:
            return 0

        try:
            nsent = self.sock.send(self.out_buffer)
        except socket.error, e:
            self.close()
            self.is_dead = True
            raise self.ConnectionError(str(e))

        self.out_buffer = buffer(self.out_buffer, nsent)

        return len(self.out_buffer)

    def send_command(self, name, kwargs={}):
        # DEBUG and _D("GearmanConnection.send_command", name, kwargs)
        pkt = self._pack_command(name, **kwargs)
        self.out_buffer += pkt
        self.send()

    def _parse_command(self, typ, data):
        """
        Returns: function name, argument dictionary
        """
        msg_spec = COMMANDS.get(typ, None)

        if not msg_spec:
            raise self.ProtocolError("Unknown message received: %d" % typ)

        nargs = len(msg_spec[1])
        data = (nargs > 0) and data.split('\x00', nargs-1) or []
        if len(data) != nargs:
            raise self.ProtocolError("Received wrong number of arguments to %s" % msg_spec[0])

        kwargs = dict(
            ((msg_spec[1][i], data[i]) for i in range(nargs))
        )

        return msg_spec[0], kwargs

    def _pack_command(self, name, **kwargs):
        msg = R_COMMANDS[name]
        data = []
        for k in msg[1]:
            v = kwargs.get(k, "")
            if v is None:
                v = ""
            data.append(str(v))
        data = "\x00".join(data)
        return "%s%s" % (struct.pack("!4sII", "\x00REQ", msg[0], len(data)), data)

    def flush(self, timeout=None): # TODO: handle connection failures
        while self.writable():
            try:
                wr = select.select([], [self], [], timeout)[1] # TODO: exc list
            except select.error, e:
                # Ignore interrupted system call, reraise anything else
                if e[0] != 4:
                    raise
            if self in wr:
                self.send()

    def send_command_blocking(self, cmd_name, cmd_args={}, timeout=None):
        self.send_command(cmd_name, cmd_args)
        self.flush(timeout)

    def recv_blocking(self, timeout=None):
        if not hasattr(self, '_command_queue'):
            self._command_queue = []

        if timeout:
            end_time = time() + timeout
        while len(self._command_queue) == 0:
            time_left = timeout and end_time - time() or 0.5
            if time_left <= 0:
                return []

            try:
                rd, wr, ex = select.select([self], self.writable() and [self] or [], [self], time_left)
            except select.error, e:
                # Ignore interrupted system call, reraise anything else
                if e[0] != 4:
                    raise
                rd = wr = ex = []

            for conn in ex:
                pass # TODO

            for conn in rd:
                for cmd in conn.recv():
                    self._command_queue.insert(0, cmd)

            for conn in wr:
                conn.send()

        return len(self._command_queue) > 0 and self._command_queue.pop() or None

    def close(self):
        self.connected = False
        try:
            self.sock.close()
        except:
            pass
        self.sock = None

    def __repr__(self):
        return "<GearmanConnection %s:%d connected=%s dead=%s>" % (self.addr[0], self.addr[1], self.connected, self.is_dead)
