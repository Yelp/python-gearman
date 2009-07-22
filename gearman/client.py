#!/usr/bin/env python

import time, select, errno

from gearman.compat import *
from gearman.connection import GearmanConnection
from gearman.task import Task, Taskset

class GearmanBaseClient(object):
    class ServerUnavailable(Exception):
        pass
    class CommandError(Exception):
        pass
    class InvalidResponse(Exception):
        pass

    def __init__(self, job_servers, prefix=None, pre_connect=False):
        """
        job_servers = ['host:post', 'host', ...]
        """
        self.prefix = prefix and "%s\t" % prefix or ""
        self.set_job_servers(job_servers, pre_connect)

    def set_job_servers(self, servers, pre_connect=False):
        # TODO: don't shut down dups. shut down old ones gracefully
        self.connections = []
        self.connections_by_hostport = {}
        for serv in servers:
            connection = GearmanConnection(serv,timeout=2)
            if pre_connect:
                try:
                    connection.connect()
                except connection.ConnectionError:
                    pass
            self.connections.append(connection)
            self.connections_by_hostport[connection.hostspec] = connection

class GearmanClient(GearmanBaseClient):
    class TaskFailed(Exception):
        pass

    def __call__(self, func, arg, uniq=None, **kwargs):
        return self.do_task(Task(func, arg, uniq, **kwargs))

    def do_task(self, task):
        """Return the result of the task or raise a TaskFailed exception on failure."""
        def _on_fail():
            raise self.TaskFailed("Task failed")
        task.on_fail.append(_on_fail)
        taskset = Taskset([task])
        if not self.do_taskset(taskset, timeout=task.timeout):
            raise self.TaskFailed("Task timeout")
        return task.result

    def dispatch_background_task(self, func, arg, uniq=None, high_priority=False):
        """Submit a background task and return its handle."""
        task = Task(func, arg, uniq, background=True, high_priority=high_priority)
        taskset = Taskset([task])
        self.do_taskset(taskset)
        return task.handle

    def get_server_from_hash(self, hsh):
        """Return a live connection for the given hash"""
        # TODO: instead of cycling through, should we shuffle the list if the first connection fails or is dead?
        first_idx = hsh % len(self.connections)
        all_dead = all(conn.is_dead for conn in self.connections)
        for idx in range(first_idx, len(self.connections)) + range(0, first_idx):
            conn = self.connections[idx]

            # if all of the connections are dead we should try reconnecting
            if conn.is_dead and not all_dead:
                continue

            try:
                conn.connect() # Make sure the connection is up (noop if already connected)
            except conn.ConnectionError:
                pass
            else:
                return conn

        raise self.ServerUnavailable("Unable to Locate Server")

    def _submit_task(self, task):
        server = self.get_server_from_hash(hash(task))
        if task.background:
            func = "submit_job_bg"
        elif task.high_priority:
            func = "submit_job_high"
        else:
            func = "submit_job"
        server.send_command(func,
            dict(func=self.prefix + task.func, arg=task.arg, uniq=task.uniq))
        server.waiting_for_handles.insert(0, task)
        return server

    def _command_handler(self, taskset, conn, cmd, args):
        # DEBUG and _D( "RECEIVED COMMAND:", cmd, args )

        handle = ('handle' in args) and ("%s//%s" % (conn.hostspec, args['handle'])) or None

        if cmd != 'job_created' and handle:
            task = taskset.get( taskset.handles.get(handle, None), None)
            if not task:
                return
            if task.is_finished:
                raise self.InvalidResponse("Task %s received %s" % (repr(task), cmd))

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
        elif cmd == 'error':
            raise self.CommandError(str(args)) # TODO make better
        else:
            raise Exception("Unexpected command: %s" % cmd)

    def do_taskset(self, taskset, timeout=None):
        """Execute a Taskset and return True iff all tasks finished before timeout."""

        # set of connections to which jobs were submitted
        taskset.connections = set(self._submit_task(task) for task in taskset.itervalues())

        taskset.handles = {}

        start_time = time.time()
        end_time = timeout and start_time + timeout or 0
        while not taskset.cancelled and not all(t.is_finished for t in taskset.itervalues()):
            timeleft = timeout and end_time - time.time() or 0.5
            if timeleft <= 0:
                taskset.cancel()
                break

            rx_socks = [c for c in taskset.connections if c.readable()]
            tx_socks = [c for c in taskset.connections if c.writable()]
            try:
                rd_list, wr_list, ex_list = select.select(rx_socks, tx_socks, taskset.connections, timeleft)
            except select.error, exc:
                # Ignore interrupted system call, reraise anything else
                if exc[0] != errno.EINTR:
                    raise
                continue

            for conn in ex_list:
                pass # TODO

            for conn in rd_list:
                for cmd in conn.recv():
                    self._command_handler(taskset, conn, *cmd)

            for conn in wr_list:
                conn.send()

        # TODO: should we fail all tasks that didn't finish or leave that up to the caller?

        return all(t.is_finished for t in taskset.itervalues())

    def get_status(self, handle):
        hostport, shandle = handle.split("//")

        server = self.connections_by_hostport[hostport]
        server.connect() # Make sure the connection is up (noop if already connected)
        server.send_command("get_status", dict(handle=shandle))
        return server.recv_blocking()[1]
