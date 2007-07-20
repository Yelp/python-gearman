import random
from select import select
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

class GearmanWorker(GearmanBaseClient):
    def __init__(self, *args, **kwargs):
        kwargs['pre_connect'] = True
        super(GearmanWorker, self).__init__(*args, **kwargs)
        self.abilities = {}

    def register_function(self, name, func, timeout=None):
        name = self.prefix + name
        self.abilities[name] = (func, timeout)
        self._can_do(self.connections, name, timeout)

    def register_class(self, clas, name=None):
        name = name or getattr(clas, 'name', "%s.%s" % (clas.__module__, clas.__name__))
        for k in clas.__dict__:
            v = getattr(clas, k)
            if callable(v) and k[0] != '_':
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
            yield conn

    def stop(self):
        self.working = False

    def work(self, stop_if_idle=False, one_task=False):
        """
        if one_task is True then
            return after completing or failing at one task
        else if stop_if_idle then
            return if no server has a task for us
        else
            work until stop() is called
        """
        self.working = True
        while self.working:
            need_sleep = True

            for conn in self._alive_connections():
                conn.send_command("grab_job")
                cmd = ('noop',)
                while cmd and cmd[0] == 'noop':
                    cmd = conn.recv_blocking(timeout=0.5)
                if not cmd:
                    # grab job timed out
                    continue

                if cmd[0] == 'no_job':
                    # _D("no_job", conn)
                    continue

                if cmd[0] != "job_assign":
                    # _D("ERROR: expected no_job or job_assigned", conn, cmd)
                    # TODO: log this, alert someone, SCREAM FOR HELP, AAAHHHHHH
                    # if cmd[0] == "error":
                    #     msg = "Error from server: %d: %s" % (cmd[1]['err_code'], cmd[1]['err_text'])
                    # else:
                    #     msg = "Was expecting job_assigned or no_job, received %s" % cmd[0]
                    conn.close()

                need_sleep = False

                # _D("Working on", cmd, cmd)
                job = GearmanJob(conn, **cmd[1])
                try:
                    func = self.abilities[cmd[1]['func']][0]
                except KeyError:
                    # TODO: log this - received work for unknown func
                    continue

                try:
                    result = func(job)
                except:
                    import traceback
                    traceback.print_exc()
                    # _D("ERROR: worker failed")
                    # TODO: log this - traceback..
                    job.fail() # TODO: handle ConnectionError
                else:
                    job.complete(result) # TODO: handle ConnectionError

                if one_task:
                    return

            is_idle = False
            if need_sleep:
                # _D("Worker: sleeping")
                is_idle = True
                for conn in self.connections:
                    conn.send_command("pre_sleep")
                rd = select([c for c in self.connections if c.readable()], [], [], 10)[0]
                is_idle = bool(rd)

            if is_idle and stop_if_idle:
                break
