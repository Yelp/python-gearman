"""
Geraman client

Overview
========

See http://www.danga.com/gearman/ for more about Gearman.

Usage Summary
=============

Example worker::

    worker = GearmanWorker(["127.0.0.1"])
    worker.register_function("echo", lambda job:job.arg)
    worker.work()

Example client::

    Single task::

        client = GearmanClient(["127.0.0.1"])
        res = client.do_task(Task("echo", "foo"))
        assert res == "foo"

    Multiple parallel tasks::

        client = GearmanClient(["127.0.0.1"])
        ts = Taskset([Task(func="echo", arg="foo")])
        client.do_taskset(ts)
        for task in ts:
            assert ts.result == "foo"

"""

__author__ = "Samuel Stauffer <samuel@descolada.com> and Kristopher Tate <kris@bbridgetech.com>"
__version__ = "0.1"
__license__ = "Python"

from gearman.client import GearmanClient
from gearman.worker import GearmanWorker
from gearman.task import Task, Taskset
