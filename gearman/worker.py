import random, sys, select
from time import time
from gearman.client import GearmanBaseClient

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
        kwargs['pre_connect'] = True

        super(GearmanWorker, self).__init__(*args, **kwargs)
        self.abilities = {}

    def register_function(self, name, func, timeout=None):
        name = self.prefix + name
        self.abilities[name] = (func, timeout)
        self._can_do(self.connections, name, timeout)

    def register_class(self, clas, name=None, decorator=None):
        obj = clas
        if not isinstance(clas, type):
            clas = clas.__class__
        name = name or getattr(obj, 'name', clas.__name__)
        for k in clas.__dict__:
            v = getattr(obj, k)
            if callable(v) and k[0] != '_':
                if decorator:
                    v = decorator(v)
                self.register_function("%s.%s" % (name,k), v)

    def _can_do(self, connections, name, timeout=None):
        cmd_name = (timeout is None) and "can_do" or "can_do_timeout"
        cmd_args = (timeout is None) and dict(func=name) or dict(func=name, timeout=timeout)
        for conn in connections:
            conn.send_command(cmd_name, cmd_args)

    def _set_abilities(self, conn):
        for name, args in self.abilities:
            self._can_do(conn, name, args[1])

    def _alive_connections(self):
        random.shuffle(self.connections)
        for conn in self.connections:
            if not conn.connected and not conn.is_dead:
                try:
                    conn.connect()
                except conn.ConnectionError:
                    continue
                else:
                    self._set_abilities(conn)
            if conn.connected:
                yield conn

    def stop(self):
        self.working = False

    def _work_connection(self, conn, hooks):
        conn.send_command("grab_job")
        cmd = ('noop',)
        while cmd and cmd[0] == 'noop':
            cmd = conn.recv_blocking(timeout=0.5)

        if not cmd or cmd[0] == 'no_job':
            return False

        if cmd[0] != "job_assign":
            # TODO: log this, alert someone, SCREAM FOR HELP, AAAHHHHHH
            # if cmd[0] == "error":
            #     msg = "Error from server: %d: %s" % (cmd[1]['err_code'], cmd[1]['err_text'])
            # else:
            #     msg = "Was expecting job_assigned or no_job, received %s" % cmd[0]
            conn.close()
            return False

        job = GearmanJob(conn, **cmd[1])
        try:
            func = self.abilities[cmd[1]['func']][0]
        except KeyError:
            # TODO: log this - received work for unknown func
            return True

        if hooks:
            hooks.start(job)
        try:
            result = func(job)
        except Exception:
            if hooks:
                hooks.fail(job, sys.exc_info())
            job.fail() # TODO: handle ConnectionError
        else:
            if hooks:
                hooks.complete(job, result)
            job.complete(result) # TODO: handle ConnectionError

        return True

    def work(self, stop_if=None, hooks=None):
        self.working = True
        stop_if = stop_if or (lambda *a,**kw:False)
        last_job_time = time()
        while self.working:
            need_sleep = True

            for conn in self._alive_connections():
                try:
                    worked = self._work_connection(conn, hooks)
                except conn.ConnectionError:
                    pass
                else:
                    if worked:
                        last_job_time = time()
                        need_sleep = False

            is_idle = False
            if need_sleep:
                is_idle = True
                for conn in self.connections:
                    conn.send_command("pre_sleep")
                alive = self._alive_connections()
                try:
                    rd, wr, ex = select.select([c for c in alive if c.readable()], [], alive, 10)
                except select.error, e:
                    # Ignore interrupted system call, reraise anything else
                    if e[0] != 4:
                        raise
                    id_idle = True
                else:
                    is_idle = not bool(rd)

            if stop_if(is_idle, last_job_time):
                self.working = False
