#!/usr/bin/env python

import time, select, errno

import gearman.util
from gearman.compat import *
from gearman.connection import GearmanConnection
from gearman.task import Task, Taskset
from gearman.protocol import *

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
            func = GEARMAN_COMMAND_SUBMIT_JOB_BG
        elif task.high_priority:
            func = GEARMAN_COMMAND_SUBMIT_JOB_HIGH
        else:
            func = GEARMAN_COMMAND_SUBMIT_JOB
        server.send_command(func,
            dict(func=self.prefix + task.func, arg=task.arg, uniq=task.uniq))
        server.waiting_for_handles.insert(0, task)
        return server

    def _command_handler(self, taskset, conn, cmd_type, cmd_args):
        # DEBUG and _D( "RECEIVED COMMAND:", cmd_type, cmd_args )
        cmd_handle = cmd_args.get('handle', None)
        if cmd_handle:
            cmd_handle = "%s//%s" % (conn.hostspec, cmd_handle)

        if cmd_type != GEARMAN_COMMAND_JOB_CREATED and cmd_handle:
            existing_handle = taskset.handles.get(cmd_handle, None)
            task = taskset.get(existing_handle, None)
            if not task:
                return

            if task.is_finished:
                raise self.InvalidResponse("Task %s received %s" % (repr(task), cmd_type))

        if cmd_type == GEARMAN_COMMAND_WORK_COMPLETE:
            task.complete(cmd_args['result'])

        elif cmd_type == GEARMAN_COMMAND_WORK_FAIL:
            if task.retries_done < task.retry_count:
                task.retries_done += 1
                task.retrying()
                task.handle = None
                taskset.connections.add(self._submit_task(task))
            else:
                task.fail()

        elif cmd_type == GEARMAN_COMMAND_WORK_STATUS:
            task.status(int(cmd_args['numerator']), int(cmd_args['denominator']))

        elif cmd_type == GEARMAN_COMMAND_JOB_CREATED:
            task = conn.waiting_for_handles.pop()
            task.handle = cmd_handle
            taskset.handles[cmd_handle] = hash( task )
            if task.background:
                task.is_finished = True

        elif cmd_type == GEARMAN_COMMAND_ERROR:
            raise self.CommandError(str(cmd_args)) # TODO make better
        else:
            raise Exception("Unexpected command: %s" % cmd_type)

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
            rd_list, wr_list, ex_list = gearman.util.select(rx_socks, tx_socks, taskset.connections, timeout=timeleft)

            for conn in ex_list:
                pass # TODO

            for conn in rd_list:
                for cmd_type, cmd_args in conn.recv():
                    self._command_handler(taskset, conn, cmd_type, cmd_args)

            for conn in wr_list:
                conn.send()

        # TODO: should we fail all tasks that didn't finish or leave that up to the caller?

        return all(t.is_finished for t in taskset.itervalues())

    def get_status(self, handle):
        hostport, shandle = handle.split("//")

        server = self.connections_by_hostport[hostport]
        server.connect() # Make sure the connection is up (noop if already connected)
        server.send_command(GEARMAN_COMMAND_GET_STATUS, dict(handle=shandle))
        cmd_type, cmd_args = server.recv_blocking()
        return cmd_args
