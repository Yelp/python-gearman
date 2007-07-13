#!/usr/bin/env python

"""
Geraman Client
"""

__author__ = "Samuel Stauffer <samuel@descolada.com> and Kristopher Tate <kris@bbridgetech.com>"
__version__ = "0.0.2"

import socket, struct, random, time
from select import select
from zlib import crc32

class s_socket (socket.socket):
    parent = None

DEFAULT_PORT = 7003
DEBUG = False

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
    class ConnectionFailed(Exception):
        pass

    class ProtocolError (Exception):
        pass

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

        self.addr = addr
        self.timeout = timeout
        self.in_buffer = ""
        self.out_buffer = ""
        self.handle = None
        self.command_handler = None # TODO: Perhaps make this a weak valued list

    @property
    def left_to_send(self):
        return len(self.out_buffer)

    def set_command_handler(self, handler):
        self.command_handler = handler

    def connect(self):
        if self.connected:
            return

        self.sock = s_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.parent = self
        self.sock.settimeout( self.timeout )
        try:
            self.sock.connect( self.addr )
        except (socket.error, socket.timeout), e:
            self.sock = None
            raise self.ConnectionFailed(str(e))

        self.connected = True
        self.sock.setblocking(0)

    def recv(self):
        self.in_buffer += self.sock.recv(1024)
        print "%r" % self.in_buffer, len(self.in_buffer)
        while self.in_buffer:
            magic, typ, data_len = struct.unpack("!4sII", self.in_buffer[:12])
            if len(self.in_buffer) < 12 + data_len:
                break

            if magic != "\x00RES":
                raise self.ProtocolError("Malformed Magic")

            self.command_handler(self, self.parse_command(typ, self.in_buffer[12:12 + data_len]))

            self.in_buffer = buffer(self.in_buffer, 12 + data_len)

    def send(self):
        if not self.connected:
            self.connect()
        noutbuffer = len(self.out_buffer)
        if noutbuffer == 0:
            return 0
        print "%r" % self.out_buffer, noutbuffer
        nsent = self.sock.send(self.out_buffer)
        self.out_buffer = (noutbuffer != nsent) and self.out_buffer[nesnt:] or ""
        
        return len(self.out_buffer)

    def send_command(self, name, **kwargs):
        pkt = self.pack_command(name, **kwargs)
        self.out_buffer += pkt
        self.send()
        
        if not self.handle and name.find('submit_job') > -1:
            def _handler(conn, cmd):
                if cmd[0] == 'job_created':
                    self.handle = cmd[1]['handle']
            
            self.command_handler = _handler

            while not self.handle:
                if select([self.sock],[],[], 5)[0]:
                    self.recv()
                
            return self.handle

    def parse_command(self, typ, data):
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

    def pack_command(self, name, **kwargs):
        msg = R_COMMANDS[name]
        data = []
        for k in msg[1]:
            v = kwargs.get(k, "")
            if v is None:
                v = ""
            data.append(str(v))
        data = "\x00".join(data)
        return "%s%s" % (struct.pack("!4sII", "\x00REQ", msg[0], len(data)), data)

def _noop(*args, **kwargs):
    pass

class Task (object):
    def __init__(self, func, arg, uniq=None, background=False, high_priority=False,
                 timeout=None, retry_count=0,
                 on_complete=_noop, on_fail=_noop,
                 on_retry=_noop, on_status=_noop):
        for k,v in locals().iteritems():
            if k == 'self': continue
            setattr(self, k, v)

        self.retries_done = 0
        self.is_finished = False
        self.result = None
#        self.handle = None
        self._hash = crc32(self.func + (self.uniq == '-' and self.arg or self.uniq or ""))
    
    def __hash__(self):
        return self._hash

class Taskset( dict ):
    handles = dict()
    def __init__(self, tasks):
        super(Taskset, self).__init__([(hash(t), t) for t in tasks])

    def add_task(self, *k, **kw):
        t = Task(*k, **kw)
        self[ hash( t ) ] = t


class Gearman(object):
    class ServerUnavailable(Exception): pass

    def __init__(self, job_servers, pre_connect = False):
        """
        job_servers = ['host:post', 'host']
        auto_connect = boolean()
        """
        self.set_job_servers(job_servers, pre_connect)

    def set_job_servers(self, servers, pre_connect = False):
        """TODO: set job servers, without shutting down dups, and shutting down old ones gracefully"""
        self.connections = []
        for serv in servers:
            host, port = (serv.find(':') == -1 and "%s:%d" % (serv, DEFAULT_PORT) or serv).split(':')
            port = int(port)
            connection = GearmanConnection(addr=(host, port))
            if pre_connect:
                connection.connect()
            self.connections.append( connection )

    def do_task(self, *args, **kwargs):
        """Returns the result of the task or raises an exception on failure"""
        def _on_fail():
            raise Exception("Task failed")
        kwargs['on_fail'] = _on_fail
        t = Task(*args, **kwargs)
        ts = Taskset( [t] )
        self.do_taskset( ts )
        return t.result

    def get_server_from_hash(self, hsh):
        # Start at server that matches hash, move up and loop around until one is found
        first_idx = hsh % len(self.connections)
        for idx in range(first_idx, len(self.connections)) + range(0, first_idx):
            server = self.connections[idx]
            try:
                server.connect() # Make sure the connection is up (noop is already connected)
            except server.ConnectionFailed:
                pass
            finally:
                return server

        raise ServerUnavailable("Unable to Locate Server")

    def do_taskset(self, taskset, timeout=None):
        def _command_handler(conn, cmd):
            task = taskset[ taskset.handles[ cmd[1]['handle'] ] ]
            if cmd[0] == 'work_complete':
                task.is_finished = True
                task.result = cmd[1]['result']
                task.on_complete( cmd[1]['result'] )

        # Assign a server to each task
        for task_hash, task in taskset.iteritems():
            server = self.get_server_from_hash( task_hash )
            handle = server.send_command(
                (task.background and "submit_job_bg") or (task.high_priority and "submit_job_high") or "submit_job",
                **dict(func=task.func, arg=task.arg, uniq=task.uniq))
            taskset.handles[ handle ] = task_hash
            server.handle = None
            server.set_command_handler(_command_handler)

        print taskset.handles

        start_time = time.time()
        end_time = timeout and start_time + timeout or 0
        rx_socks = [s.sock for s in self.connections]
        tx_socks = [s.sock for s in self.connections if s.left_to_send > 0]
        while not timeout or time.time() < end_time:
            rd, wr = select(
                rx_socks,
                tx_socks,
                [], 0.5)[:2] # TODO: timeout

            if not rd:
                all_finished = True
                for t in taskset.values():
                    if not t.is_finished:
                        all_finished = False
                if all_finished:
                    break

            for sock in rd:
                sock.parent.recv()


if __name__ == "__main__":
    import sys
    hosts = ["207.7.148.210:19000"]

    client = Gearman( hosts )
    d = client.do_task("foo", "bar",
        on_complete=lambda blah:sys.stdout.write("DONE: %s\n" % blah),
        on_status=lambda num,den:sys.stdout.write("STATUS: %s/%s\n" % (num, den)))
    print "DUDE:", d