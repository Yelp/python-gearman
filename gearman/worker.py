import random, sys, select, logging
from time import time

from gearman.compat import *
from gearman.client import GearmanBaseClient

log = logging.getLogger("gearman")

class GearmanJob(object):
    def __init__(self, conn, func, arg, handle):
        self.func = func
        self.arg = arg
        self.handle = handle
        self.conn = conn

    def status(self, numerator, denominator):
        self.conn.send_command_blocking("work_status", dict(handle=self.handle, numerator=numerator, denominator=denominator))

    def complete(self, result):
        self.conn.send_command_blocking("work_complete", dict(handle=self.handle, result=result))

    def fail(self):
        self.conn.send_command_blocking("work_fail", dict(handle=self.handle))

    def __repr__(self):
        return "<GearmanJob func=%s arg=%s handle=%s conn=%s>" % (self.func, self.arg, self.handle, repr(self.conn))

class GearmanWorker(GearmanBaseClient):
    def __init__(self, *args, **kwargs):
        super(GearmanWorker, self).__init__(*args, **kwargs)
        self.abilities = {}

    def register_function(self, name, func, timeout=None):
        """Register a function with gearman with an optional default timeout.
        """
        name = self.prefix + name
        self.abilities[name] = (func, timeout)

    def register_class(self, clas, name=None, decorator=None):
        """Register all the methods of a class or instance object with
        with gearman.
        
        'name' is an optional prefix for function names (name.method_name)
        """
        obj = clas
        if not isinstance(clas, type):
            clas = clas.__class__
        name = name or getattr(obj, 'name', clas.__name__)
        for k in clas.__dict__:
            v = getattr(obj, k)
            if callable(v) and k[0] != '_':
                if decorator:
                    v = decorator(v)
                self.register_function("%s.%s" % (name, k), v)

    def _can_do(self, connection, name, timeout=None):
        cmd_name = (timeout is None) and "can_do" or "can_do_timeout"
        cmd_args = (timeout is None) and dict(func=name) or dict(func=name, timeout=timeout)
        connection.send_command(cmd_name, cmd_args)

    def _set_abilities(self, conn):
        for name, args in self.abilities.iteritems():
            self._can_do(conn, name, args[1])

    @property
    def alive_connections(self):
        """Return a shuffled list of connections that are alive,
        and try to reconnect to dead connections if necessary."""
        random.shuffle(self.connections)
        all_dead = all(conn.is_dead for conn in self.connections)
        alive = []
        for conn in self.connections:
            if not conn.connected and (not conn.is_dead or all_dead):
                try:
                    conn.connect()
                except conn.ConnectionError:
                    continue
                else:
                    conn.sleeping = False
                    self._set_abilities(conn)
            if conn.connected:
                alive.append(conn)
        return alive

    def stop(self):
        self.working = False

    def _work_connection(self, conn, hooks=None):
        conn.send_command("grab_job")
        cmd = ('noop',)
        while cmd and cmd[0] == 'noop':
            cmd = conn.recv_blocking(timeout=0.5)

        if not cmd or cmd[0] == 'no_job':
            return False

        if cmd[0] != "job_assign":
            if cmd[0] == "error":
                log.error("Error from server: %s: %s" % (cmd[1]['err_code'], cmd[1]['err_text']))
            else:
                log.error("Was expecting job_assigned or no_job, received %s" % cmd[0])
            conn.mark_dead()
            return False

        job = GearmanJob(conn, **cmd[1])
        try:
            func = self.abilities[cmd[1]['func']][0]
        except KeyError:
            log.error("Received work for unknown function %s" % cmd[1])
            return True

        if hooks:
            hooks.start(job)
        try:
            result = func(job)
        except Exception:
            if hooks:
                hooks.fail(job, sys.exc_info())
            job.fail()
        else:
            if hooks:
                hooks.complete(job, result)
            job.complete(result)

        return True

    def work(self, stop_if=None, hooks=None):
        """Loop indefinitely working tasks from all connections."""
        self.working = True
        stop_if = stop_if or (lambda *a, **kw:False)
        last_job_time = time()
        while self.working:
            need_sleep = True

            # Try to grab work from all alive connections
            for conn in self.alive_connections:
                if conn.sleeping:
                    continue

                try:
                    worked = self._work_connection(conn, hooks)
                except conn.ConnectionError, exc:
                    log.error("ConnectionError on %s: %s" % (conn, exc))
                else:
                    if worked:
                        last_job_time = time()
                        need_sleep = False

            # If no tasks were handled then sleep and wait for the server
            # to wake us with a 'noop'
            is_idle = False
            if need_sleep:
                is_idle = True
                alive = self.alive_connections
                for conn in alive:
                    if not conn.sleeping:
                        conn.send_command("pre_sleep")
                        conn.sleeping = True
                try:
                    rd, wr, ex = select.select([c for c in alive if c.readable()], [], alive, 10)
                except select.error, e:
                    # Ignore interrupted system call, reraise anything else
                    if e[0] != 4:
                        raise
                else:
                    is_idle = not bool(rd)

                    for c in ex:
                        log.error("Exception on connection %s" % c)
                        c.mark_dead()

                    for c in rd:
                        c.sleeping = False

            if stop_if(is_idle, last_job_time):
                self.working = False
