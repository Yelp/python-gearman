#!/usr/bin/env python

"""
Geraman Client
"""

__author__ = "Samuel Stauffer (samuel@descolada.com)"
__version__ = "0.0.1"

import socket, time, struct, random
from zlib import crc32
from twisted.internet import defer, reactor
from twisted.internet.protocol import Protocol, ClientCreator, ClientFactory

DEFAULT_PORT = 7003
DEBUG = False

def start_reactor():
    if not reactor.running:
        reactor.startRunning()

def stop_reactor():
    if reactor.running:
        reactor.callLater(0, reactor.stop)
        while reactor.running:
            reactor.iterate(1)

# from atexit import register
# register(stop_reactor)

class GearmanProtocol(Protocol):
    MESSAGES = {
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

    def __init__(self, factory, *args, **kwargs):
        self.factory = factory

        self.r_messages = dict(
            (m[0], (mid, m[1])) for mid,m in self.MESSAGES.iteritems()
        )

    ####

    def connectionMade(self):
        # self.transport.loseConnection()
        self.buffer = ""
        self.factory.clientConnected(self)

    # def connectionLost(self):
    #     if DEBUG:
    #         print "CONNECTION LOST", self.hostid

    def dataReceived(self, data):
        self.buffer += data
        while len(self.buffer) >= 12:
            magic,type,data_len = struct.unpack(">4sLL", buffer(self.buffer, 0, 12))
            if len(self.buffer) < 12+data_len:
                break
    
            if magic == "\x00RES":
                self.on_message(type, self.buffer[12:12+data_len]) #buffer(self.buffer, 12, data_len))
            else:
                raise Exception("TODO")
    
            self.buffer = buffer(self.buffer, 12+data_len)

    ####

    def send_message(self, name, **kwargs):
        if DEBUG:
            print "SEND", self.hostid, name, kwargs
        msg = self.r_messages[name]
        data = []
        for k in msg[1]:
            v = kwargs.get(k, "")
            if v is None:
                v = ""
            data.append(str(v))
        self.send_request(msg[0], "\x00".join(data))

    def on_message(self, type, data):
        msg_spec = self.MESSAGES.get(type)
        if not msg_spec:
            raise Exception("Unknown message received: %d" % type)

        nargs = len(msg_spec[1])
        data = data.split('\x00', nargs-1)
        args = dict(
            ((msg_spec[1][i], data[i]) for i in range(nargs))
        )

        if DEBUG:
            print "RECEIVE", self.hostid, msg_spec[0], args

        getattr(self.factory.client, 'on_'+msg_spec[0])(self.hostid, **args)


    def send_request(self, type, data=None):
        ndata = (data and len(data)) or 0
        pkt = struct.pack(">4sLL", "\x00REQ", type, ndata)
        if data:
            pkt += data
        self.transport.write(pkt)

class GearmanFactory(ClientFactory):
    def __init__(self, client):
        self.client = client

    def startedConnecting(self, connector):
        self.client.on_server_connecting("%s:%d" % (connector.host, connector.port))

    def buildProtocol(self, addr):
        proto = GearmanProtocol(self)
        proto.hostid = "%s:%d" % (addr.host, addr.port)
        return proto

    def clientConnected(self, proto):
        self.client.on_server_connected(proto.hostid, proto)

    def clientConnectionLost(self, connector, reason):
        self.client.on_server_connection_lost("%s:%d" % (connector.host, connector.port), reason)
        ClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        self.client.on_server_connection_failed("%s:%d" % (connector.host, connector.port), reason)
        ClientFactory.clientConnectionFailed(self, connector, reason)

class ClientJob(object):
    def __init__(self, handle, deferred, client):
        self.handle = handle
        self.deferred = deferred
        self.client = client
        self.hostid, self.host_handle = handle.split('//', 1)
        self.on_status = [self._on_status]
        self.numerator = -1
        self.denominator = -1

    def _on_status(self, handle, numerator, denominator):
        self.numerator = numerator
        self.denominator = denominator

    @property
    def status(self):
        return self.numerator, self.denominator

    def __str__(self):
        return self.handle

class WorkerJob(object):
    def __init__(self, hostid, func, arg, handle, client):
        self.hostid = hostid
        self.handle = handle
        self.arg = arg
        self.func = func
        self.client = client

    def complete(self, result):
        self.client.server_cmd(self.hostid, "work_complete", handle=self.handle, result=result)

    def fail(self):
        self.client.server_cmd(self.hostid, "work_fail", handle=self.handle)

    def set_status(self, numerator, denominator):
        self.client.server_cmd(self.hostid, "work_status", handle=self.handle, numerator=numerator, denominator=denominator)

class GearmanClient(object):
    def __init__(self, job_servers, prefix=None):
        start_reactor()

        self.prefix = (prefix and prefix+"\t") or ""
        self.job_servers = dict()
        self.factory = GearmanFactory(self)
        self.set_job_servers(job_servers)
        self.funcs = {}
        self.waiting_for_handles = {}
        self.pending_jobs = {}
        self.waiting_for_status = {}

    def set_job_servers(self, job_servers):
        """TODO: set job servers, without shutting down dups, and shutting down old ones gracefully"""
        job_servers = [(':' in s and s) or "%s:%d" % (s, DEFAULT_PORT) for s in job_servers]
        for s in job_servers:
            host,port = s.split(':')
            port = int(port)
            server = dict(
                state="connecting",
                reconnects=0,
                last_reconnect_attempt=0,
                host=host,
                port=port,
                proto=None,
                hostid="%s:%d" % (host,port),
            )
            self.job_servers[server['hostid']] = server
            self.connect(server)

    def on_server_connecting(self, hostid):
        if DEBUG:
            print "CONNECTING", hostid
        server = self.job_servers[hostid]
        server['state'] = "connecting"

    def on_server_connected(self, hostid, proto):
        if DEBUG:
            print "CONNECTED", hostid
        server = self.job_servers[hostid]
        server['state'] = "connected"
        server['proto'] = proto
        # for cmds in server['queue']:
        #     proto.send_message(cmds[0], **cmds[1])

    def on_server_connection_lost(self, hostid, reason=None):
        if DEBUG:
            print "CONNECTION LOST", hostid
        server = self.job_servers[hostid]
        server['state'] = "connecting"
        server['proto'] = None
        reactor.callLater(random.randint(1,5), self.connect, server)

    def on_server_connection_failed(self, hostid, reason=None):
        if DEBUG:
            print "CONNECTION FAILED", hostid
        server = self.job_servers[hostid]
        server['state'] = "dead"
        server['proto'] = None
        reactor.callLater(random.randint(5,10), self.connect, server)

    def connect(self, server):
        server['reconnects'] += 1
        server['last_reconnect_attempt'] = time.time()
        reactor.connectTCP(server['host'], server['port'], self.factory, timeout=0.2)

    def server_cmd(self, server, cmd, **kwargs):
        if isinstance(server, basestring):
            server = self.job_servers[server]
        server['proto'].send_message(cmd, **kwargs)
        reactor.iterate(0.1) # TODO: temporary - try to give enough time for message to go out.. allows tasks to get to workers

    def server_cmd_all(self, *args, **kwargs):
        for s in self.job_servers.values():
            if self.check_server(s):
                self.server_cmd(s, *args, **kwargs)

    def check_server(self, server, retries=2):
        """Allow the server connection a bit of time to come up, and return True if it is connected."""
        # TODO: if reactor is running then just return "server['state'] == 'connected'"
        # TODO: should we allow this function to be called recursively?
        # TODO: ugh, get rid of this function.. LAAAAAME
        while server['state'] == 'connecting' and retries > 0:
            reactor.iterate(0.1)
            retries -= 1
        return server['state'] == "connected"

    def _get_hashed_server(self, key):
        servers = self.job_servers.values()
        if not key:
            return random.choice(servers)
        hash = (crc32(key) >> 16) & 0x7fff
        i = hash % len(servers)
        server = servers[i]
        if not self.check_server(server):
            for ser in servers:
                if ser != server and self.check_server(ser):
                    return ser
        else:
            return server

    # Client

    def do_task(self, func, arg, uniq=None, background=False, high_priority=False, on_success=None, on_fail=None, on_status=None):
        """Returns a deferred that gets called with the handle and new deferred when the job is created"""
        server = self._get_hashed_server(func + (uniq == '-' and arg) or uniq or "")

        func = self.prefix + func

        cmd = (background and "submit_job_bg") or (high_priority and "submit_job_high") or "submit_job"
        self.server_cmd(server, cmd, func=func, arg=arg, uniq=uniq)

        if DEBUG:
            print "DO_TASK", server['hostid']

        d = defer.Deferred()
        self.waiting_for_handles.setdefault(server['hostid'], []).insert(0, (d, on_success, on_fail, on_status))
        return d

    def wait_for_jobs(self, timeout=None):
        """Return True if all jobs completed or False if timed out."""
        # TODO: would make more sense to start the reactor and set a timer and a signal to stop the reactor
        #       when all jobs complete.
        if timeout is not None:
            t = time.time() + timeout
        while sum(map(len, self.pending_jobs.itervalues())) > 0 or \
              sum(map(len, self.waiting_for_handles.itervalues())) > 0:
            if timeout is not None and t < time.time():
                return False
            reactor.iterate(0.1)
        return True

    def get_status(self, handle):
        raise NotImplementedError()

    def on_job_created(self, hostid, handle):
        d,on_success,on_fail,on_status = self.waiting_for_handles[hostid].pop()

        job_queue = self.pending_jobs.setdefault(hostid, {})
        job = job_queue.get(handle)
        if not job:
            nd = defer.Deferred()
            job = ClientJob(
                client = self,
                deferred = nd,
                handle = "%s//%s" % (hostid, handle),
            )
            job_queue[handle] = job

        if on_status:
            job.on_status.append(on_status)

        if on_success and on_fail:
            nd.addCallbacks(on_success, lambda exc:on_fail())
        elif on_success:
            nd.addCallback(on_success)
        elif on_fail:
            ns.addErrback(lambda exc:on_fail())

        d.callback(job)

    def on_work_complete(self, hostid, handle, result):
        job_queue = self.pending_jobs[hostid]
        job = job_queue.pop(handle, None)
        if not job:
            # Ignore duplicates, most likely because of job merging based on uniq
            if DEBUG:
                print "WORK COMPLETE ignore duplicate:", hostid, handle
            return

        job.deferred.callback(result)

    class WorkFailError(Exception):
        pass
    def on_work_fail(self, hostid, handle):
        job_queue = self.pending_jobs[hostid]
        job = job_queue.pop(handle, None)
        if not job:
            # Ignore duplicates, most likely because of job merging based on uniq
            if DEBUG:
                print "WORK FAIL ignore duplicate:", hostid, handle
            return

        exc = self.WorkFailError()
        job.deferred.errback(exc)

    def on_work_status(self, hostid, handle, numerator, denominator):
        job_queue = self.pending_jobs[hostid]
        job = job_queue.get(handle)
        handle = "%s//%s" % (hostid, handle)
        for hook in job.on_status:
            hook(numerator, denominator)

    def get_status(self, handle):
        d = self.waiting_for_status.setdefault(handle, defer.Deferred())
        hostid,handle = handle.split('//')
        self.server_cmd(hostid, "get_status", handle=handle)
        return d

    def on_status_res(self, hostid, handle, known, running, numerator, denominator):
        fullhandle = "%s//%s" % (hostid, handle)
        d = self.waiting_for_status.pop(fullhandle)
        d.callback(dict(
            handle=fullhandle,
            known=known,
            running=running,
            numerator=numerator,
            denominator=denominator,
        ))

    def on_error(self, hostid, err_code, err_text):
        print "ERROR:", err_code, err_text # TODO

    def close(self):
        for s in self.job_servers.itervalues():
            s['proto'].transport.loseConnection()

class GearmanWorker(GearmanClient):
    def __init__(self, *args, **kwargs):
        start = kwargs.pop('start', False)

        super(GearmanWorker, self).__init__(*args, **kwargs)

        for k,v in self.__class__.__dict__.iteritems():
            name = getattr(v, 'worker', None)
            if name:
                self.register_function(name, getattr(self, k))
        self.last_server = 0
        self.last_job_server = 0
        self.working = False

        if start:
            self.start_working()

    @staticmethod
    def function(name):
        def _register_function(func):
            func.worker = name
            return func
        return _register_function

    def on_server_connected(self, hostid, proto):
        super(GearmanWorker, self).on_server_connected(hostid, proto)
        server = self.job_servers[hostid]
        for name,func in self.funcs.iteritems():
            self.server_cmd_all("can_do", func=name)

    def all_yours(self):
        self.server_cmd_all('all_yours')

    def register_function(self, name, func):
        self.server_cmd_all("can_do", func=self.prefix+name)
        self.funcs[self.prefix+name] = func

    def register_class(self, clas, name=None):
        name = name or getattr(clas, 'name', "%s.%s" % (clas.__module__, clas.__name__))
        for k in clas.__dict__:
            v = getattr(clas, k)
            if callable(v) and k[0] != '_':
                self.register_function("%s.%s" % (name,k), v)

    def on_job_assign(self, hostid, handle, func, arg):
        self.last_job_server = self.last_server

        job = WorkerJob(hostid=hostid, handle=handle, func=func, arg=arg, client=self)
        try:
            d = self.funcs[func](job)
        except:
            import traceback
            traceback.print_exc() # TODO: log instead of stdout
            job.fail()
        else:
            if isinstance(d, basestring) or d is None:
                job.complete(d or "")
            else:
                d.addCallback(lambda res:job.complete(res))
                d.addErrback(lambda exc:job.fail())

        self.work()

    def on_noop(self, hostid):
        self.work()

    def on_no_job(self, hostid):
        if self.last_server == self.last_job_server:
            # no jobs available from any server, time to sleep
            self.server_cmd_all('pre_sleep')
            reactor.callLater(10, self.work)
        else:
            self.work()

    def start_working(self):
        self.working = True
        self.work()

    def stop_working(self):
        self.working = False

    def work(self):
        if not self.working:
            return

        self.last_server = (self.last_server + 1) % len(self.job_servers)
        server = self.job_servers.values()[self.last_server]

        if server['state'] != 'connected':
            reactor.callLater(0, self.work)
        else:
            self.server_cmd(server, 'grab_job')

    def run(self):
        reactor.run()

if __name__ == "__main__":
    import sys

    hosts = ["127.0.0.1:7003"]

    class TestWorker(object):
        @staticmethod
        def echo(job):
            return job.arg

    # Worker
    worker = GearmanWorker(job_servers=hosts)
    worker.register_class(TestWorker)
    worker.start_working()
    # worker.run()

    # Client
    client = GearmanClient(job_servers=hosts)
    d = client.do_task("__main__.TestWorker.echo", "test", "-",
        on_success=lambda blah:sys.stdout.write("DONE: %s\n" % blah),
        on_status=lambda num,den:sys.stdout.write("STATUS: %s/%s\n" % (num, den)))
    def _error(exc):
        print "ERROR: %s" % exc.type
    def _created(job):
        print "CREATED: %s" % job.handle
        # job.deferred.addCallbacks(
        #     lambda res:sys.stdout.write("DONE: %s\n" % res),
        #     _error,
        # ),
    d.addCallbacks(_created, _error)
    client.wait_for_jobs()
