==============
python-gearman
==============

python-gearman is a pure Python Gearman library.

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
    >>> from scrubber import Scrubber
    >>> scrubber = Scrubber(autolink=True)
    >>> scrubber.scrub("<script>alert('foo');</script><p>bar, www.google.com</p>")
    u'<p>bar, <a href="http://www.google.com" rel="nofollow">www.google.com</a></p>'
    >>>
