==============
python-gearman
==============

Pure Python Gearman library.

See http://www.danga.com/gearman/ for more about Gearman.

Installation
============

Stable releases of python-gearman can be installed using
``easy_install`` or ``pip``.

Source
======

You can find the latest version of scrubber at
http://github.com/samuel/python-gearman

Example
=======

Example worker::

    worker = GearmanWorker(["127.0.0.1"])
    worker.register_function("echo", lambda job:job.arg)
    worker.work()

Example client (single task)::

        client = GearmanClient(["127.0.0.1"])
        res = client.do_task(Task("echo", "foo"))
        assert res == "foo"

Example client (multiple parallel tasks)::

    client = GearmanClient(["127.0.0.1"])
    ts = Taskset([
        Task(func="echo", arg="foo"),
        Task(func="echo", arg="bar"),
    ])
    client.do_taskset(ts)
    for task in ts.values():
        assert task.result == task.arg
