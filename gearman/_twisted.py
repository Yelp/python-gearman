#!/usr/bin/env python

from __future__ import with_statement

import time, select, errno

from gearman.compat import *
from gearman.connection import GearmanConnection
from gearman.task import Task, Taskset
from gearman.client import GearmanClient

from twisted.internet import abstract, defer, reactor

class _ConnectionWrapper(abstract.FileDescriptor):
    def __init__(self, conn, onRead, *args, **kwargs):
        abstract.FileDescriptor.__init__(self, *args, **kwargs)
        self.conn = conn
        self.onRead = onRead
        self.reading = False
        self.writing = False

    def startReading(self):
        if not self.reading:
            reactor.addReader(self)
            self.reading = True

    def stopReading(self):
        if self.reading:
            reactor.removeReader(self)
            self.reading = False

    def startWriting(self):
        if not self.writing:
            reactor.addWriter(self)
            self.writing = True

    def stopWriting(self):
        if self.writing:
            reactor.removeWriter(self)
            self.writing = False

    def setState(self):
        if self.conn.readable():
            self.startReading()
        else:
            self.stopReading()
        if self.conn.writable():
            self.startWriting()
        else:
            self.stopWriting()    

    def __enter__(self):
        self.setState()

    def __exit__(self, type, value, traceback):
        self.stopReading()
        self.stopWriting()

    def doWrite(self):
        self.conn.send()
        self.setState()

    def doRead(self):
        for cmd in self.conn.recv():
            self.onRead(*cmd)
        self.setState()

    def fileno(self):
        return self.conn.fileno()

class Client(GearmanClient):
    @defer.inlineCallbacks
    def do_task(self, task):
        """Return the result of the task or raise a TaskFailed exception on failure."""

        rtn = defer.Deferred()

        def _on_fail():
            rtn.errback(self.TaskFailed("Task failed"))

        task.on_fail.append(_on_fail)
        task.on_complete.append(rtn.callback)

        taskset = Taskset([task])
        taskset.handles = {}
        
        c = self._submit_task(task)
        taskset.connections = set([c])

        def _do_cmd(*args):
            return self._command_handler(taskset, c, *args)

        with _ConnectionWrapper(c, _do_cmd):
            rtn = yield rtn

        defer.returnValue(rtn)

    def dispatch_background_task(self, func, arg, uniq=None, high_priority=False):
        assert False, 'Not Implemented, use do_task(...)'

    def do_taskset(self, taskset, timeout=None):
        assert False, 'Not Implemented, use do_task(...) and DeferredList()'

    def get_status(self, handle):
        assert False, 'Not Implemented'
