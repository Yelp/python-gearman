#!/usr/bin/env python

"""
Geraman Client
"""

__author__ = "Samuel Stauffer <samuel@descolada.com> and Kristopher Tate <kris@bbridgetech.com>"
__version__ = "0.0.3"

import socket, struct, random, time
from select import select
from zlib import crc32

# Compatability with pre 2.5
try:
    all
except NameError:
    def all(iter):
        for v in iter:
            if not v:
                return False
        return True



DEFAULT_PORT = 7003
DEBUG = True

if DEBUG:
    def _D(p, *k):
        if not k:
            print "\nM:", p, "\n"
        else:
            print p, repr(k)
else:
    def _D(*a, **kw):
        pass

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

#class

class GearmanConnection(object):
    class ConnectionError(Exception): pass
    class ProtocolError(Exception): pass

    def __init__(self, addr=None, sock=None, timeout=None):
        """
        A connection to a Gearman server.
        """
        if not sock:
            self.sock = None
            self.connected = False
        else:
            self.sock = sock
            self.connected = True

        self.addr     = addr
        self.hostspec = ":".join(str(x) for x in addr)
        self.timeout  = timeout

        self.in_buffer  = ""
        self.out_buffer = ""
        self.is_dead    = False
        self.waiting_for_handles = []

    def fileno(self):
        return self.sock.fileno()

    def writable(self):
        return len(self.out_buffer) != 0

    def readable(self):
        return self.connected

    def connect(self):
        """Connect to the server. Raises ConnectionError if connection fails."""

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

        self.is_dead = False
        self.connected = True
        self.sock.setblocking(0)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, struct.pack("L", 1))

    def is_dead():
        def fget(self):
            if self._is_dead and time.time() >= self._retry_at:
                self._is_dead = False
            return self._is_dead
        def fset(self, value):
            self._is_dead = value
            self._retry_at = value and time.time() + 60*2 or None # TODO: should be configurable
        return locals()
    is_dead = property(**is_dead())

    def recv(self):
        """
        Returns a list of commands: [(cmd_name, cmd_args), ...]
        Raises ConnectionError if the connection dies.
            or ProtocolError if parsing of a command fails.
        """

        assert self.connected

        data = ''
        try:
            data = self.sock.recv(4096)
        except socket.error, e:
            if e.args[0] == 35: # would block / EAGAIN
                return
            raise
        else:
            if not data:
                self.close()
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
            raise self.ConnectionError(str(e))

        self.out_buffer = buffer(self.out_buffer, nsent)

        return len(self.out_buffer)

    def send_command(self, name, kwargs):
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
        data = data.split('\x00', nargs-1)
        if len(data) != nargs:
            raise self.ProtocolError("Wrong number of arguments to %s" % msg_spec[0])

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

    def close(self):
        self.connected = False
        try:
            self.sock.close()
        except:
            pass
        self.sock = None

    def __repr__(self):
        return "<GearmanConnection %s:%d connected=%s dead=%s>" % (self.addr[0], self.addr[1], self.connected, self.is_dead)

class Task(object):
    hooks = ('on_complete', 'on_fail', 'on_retry', 'on_status', 'on_post')

    def __init__(self, func, arg, uniq=None, background=False, high_priority=False,
                 timeout=None, retry_count=0, **kwargs):
        for hook in self.hooks:
            setattr(self, hook, hook in kwargs and [kwargs[hook]] or [])

        self.func          = func
        self.arg           = arg
        self.uniq          = uniq
        self.background    = background
        self.high_priority = high_priority

        self.retries_done = 0
        self.is_finished  = False
        self.handle       = None
        self.result       = None
        self._hash        = crc32(self.func + (self.uniq == '-' and self.arg or self.uniq or str(random.randint(0,999999))))

    def __hash__(self):
        return self._hash

    def merge_hooks(self, task2):
        for hook in self.hooks:
            getattr(self, hook).extend(getattr(task2, hook))

    def complete(self, result):
        self.result = result
        for func in self.on_complete:
            func(result)
        self._finished()

    def fail(self):
        for func in self.on_fail:
            func()
        self._finished()

    def status(self, numerator, denominator):
        for func in self.on_status:
            func(numerator, denominator)

    def retrying(self):
        for func in self.on_retry:
            func()

    def _finished(self):
        self.is_finished = True
        for func in self.on_post:
            func()
        for hook in self.hooks:
            delattr(self, hook) # TODO: perhaps should just clear the lists?

    def __repr__(self):
        return "<Task func='%s'>" % self.func

class Taskset(dict):
    def __init__(self, tasks=[]):
        super(Taskset, self).__init__((hash(t), t) for t in tasks)
        self.handles = {}
        self.cancelled = False

    def add(self, task):
        self[hash(task)] = task

    def add_task(self, *args, **kwargs):
        self.add(Task(*args, **kargs))

    def cancel(self):
        self.cancelled = True

    def __or__(self, taskset2):
        for k,v in taskset2.iteritems():
            if k in self:
                self[k].merge_hooks(v)
            else:
                self[k] = v # TODO: should clone the task rather than just making a reference
        return self

class Gearman(object):
    class ServerUnavailable(Exception): pass
    class CommandError(Exception): pass
    class InvalidResponse(Exception): pass

    def __init__(self, job_servers, prefix=None, pre_connect=False):
        """
        job_servers = ['host:post', 'host', ...]
        """
        self.prefix = prefix and "%s\t" % prefix or ""
        self.set_job_servers(job_servers, pre_connect)

    def set_job_servers(self, servers, pre_connect = False):
        # TODO: don't shut down dups and shut down old ones gracefully
        self.connections = []
        for serv in servers:
            host, port = (':' not in serv and "%s:%d" % (serv, DEFAULT_PORT) or serv).split(':')
            port = int(port)
            connection = GearmanConnection(addr=(host, port))
            if pre_connect:
                try:
                    connection.connect()
                except connection.ConnectionError:
                    pass # TODO: should we not add it to the list? it IS marked as dead but perhaps we don't want it at all
            self.connections.append( connection )

    def do_task(self, task):
        """Returns the result of the task or raises an exception on failure"""
        def _on_fail():
            raise Exception("Task failed")
        task.on_fail.append([_on_fail])
        ts = Taskset( [task] )
        self.do_taskset( ts )
        return t.result

    # def dispatch_background_task(self, *args, **kwargs):
    #     pass

    def get_server_from_hash(self, hsh):
        """Returns a live connection for the given hash"""
        # TODO: instead of cycling through we should shuffle the list if the first connection fails or is dead
        first_idx = hsh % len(self.connections)
        for idx in range(first_idx, len(self.connections)) + range(0, first_idx):
            conn = self.connections[idx]
            if conn.is_dead:
                continue

            try:
                conn.connect() # Make sure the connection is up (noop is already connected)
            except conn.ConnectionError:
                pass
            else:
                return conn

        raise self.ServerUnavailable("Unable to Locate Server")

    def _submit_task(self, task):
        server = self.get_server_from_hash(hash(task))
        server.send_command(
            (task.background and "submit_job_bg") or
                (task.high_priority and "submit_job_high") or
                "submit_job",
            dict(func=self.prefix + task.func, arg=task.arg, uniq=task.uniq))
        server.waiting_for_handles.insert(0,task)
        return server

    def _command_handler(self, taskset, conn, cmd, args):
        DEBUG and _D( "COMMAND:", cmd, args )

        handle = ('handle' in args) and ("%s//%s" % (conn.hostspec, args['handle'])) or None

        if cmd != 'job_created' and handle:
            task = taskset.get( taskset.handles.get(handle, None), None)
            if not task or task.is_finished:
                raise self.InvalidResponse()

        if cmd == 'work_complete':
            task.complete(args['result'])
        elif cmd == 'work_fail':
            if task.retries_done < task.retry_count:
                task.retries_done += 1
                task.retrying()
                task.handle = None
                taskset.connections.add(self._submit_task(task))
            else:
                task.fail()
        elif cmd == 'work_status':
            task.status(int(args['numerator']), int(args['denominator']))
        elif cmd == 'job_created':
            task = conn.waiting_for_handles.pop()
            task.handle = handle
            taskset.handles[handle] = hash( task )
            if task.background:
                task.is_finished = True
            _D( "Assigned H[%r] from S[%s] to T[%s]" % (handle, '%s:%s' % conn.addr, hash(task) ))
        elif cmd == 'error':
            raise self.CommandError(str(args)) # TODO make better
        else:
            raise Exception("Unexpected command: %s" % cmd)

        # _D( "HANDLES:", taskset.handles )

    def do_taskset(self, taskset, timeout=None):
        """Execute a taskset. Returns True iff all tasks finished (to success or failure)."""
        taskset.connections = set()
        for task in taskset.itervalues():
            taskset.connections.add(self._submit_task(task))

        DEBUG and _D( "Tasks in set:", taskset.values() )
        DEBUG and _D( "Servers used for taskset:", taskset.connections )

        start_time = time.time()
        end_time = timeout and start_time + timeout or 0
        while not taskset.cancelled and not all(t.is_finished for t in taskset.itervalues()):
            timeleft = timeout and end_time - time.time() or 0.5
            if timeleft <= 0:
                taskset.cancel()
                break

            rx_socks = [c for c in taskset.connections if c.readable()]
            tx_socks = [c for c in taskset.connections if c.writable()]
            rd, wr, ex = select(rx_socks, tx_socks, taskset.connections, timeleft)

            for conn in ex:
                pass # TODO

            for conn in rd:
                for cmd in conn.recv():
                    self._command_handler(taskset, conn, *cmd)

            for conn in wr:
                conn.send()

        return all(t.is_finished for t in taskset.itervalues())

if __name__ == "__main__":
    import sys
    hosts = ["207.7.148.210:19000", "75.42.252.242:19000"]
    # hosts = ["75.42.252.242:19000", "127.0.0.1:5559"]

    client = Gearman( hosts )
    # d = client.do_task(Task("foo", "bar"))
    # print "do_task:", d

    t1 = Task("foo", "bar", on_complete=lambda res:sys.stdout.write("bar: %s\n" % res))
    ts1 = Taskset( [t1] )
    t2 = Task("foo", "pie", retry_count=3, high_priority=True, on_complete=lambda res:sys.stdout.write("pie: %s\n" % res))
    ts2 = Taskset( [t2] )
    ts = ts1 | ts2

    # ts = Taskset()
    # for i in range(20):
    #     t = Task("foo", "pie%d" % i, retry_count=3, high_priority=True, on_complete=lambda res:sys.stdout.write("pie: %s\n" % res))
    #     ts.add(t)

    client.do_taskset( ts, timeout=5 )

    for t in ts.itervalues():
        print "DUDE:", t.result
