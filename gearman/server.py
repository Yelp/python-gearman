
import logging
import random
import time
import asyncore
import socket
from collections import deque
from gearman.protocol import DEFAULT_PORT, ProtocolError, parse_command, pack_command

class GearmanServerClient(asyncore.dispatcher):
    def __init__(self, sock, addr, server, manager):
        asyncore.dispatcher.__init__(self, sock)
        self.addr = addr
        self.server = server
        self.manager = manager
        self.in_buffer = ""
        self.out_buffer = ""
        manager.register_client(self)

    def writable(self):
        return len(self.out_buffer) != 0

    def handle_close(self):
        self.close()
        self.manager.deregister_client(self)

    def handle_read(self):
        data = self.recv(8192)
        if not data:
            self.close()
            return

        self.in_buffer += data

        commands = []
        while True:
            try:
                func, args, cmd_len = parse_command(self.in_buffer, response=False)
            except ProtocolError, exc:
                logging.error("[%s] ProtocolError: %s" % (self.addr, str(exc)))
                self.close()
                return

            if not func:
                break

            self.handle_command(func, args)

            self.in_buffer = buffer(self.in_buffer, cmd_len)

    def handle_command(self, func, args):
        if func == "echo_req":
            self.send_command("echo_res", args)
        elif func == "submit_job":
            handle = self.manager.add_job(self, **args)
            self.send_command("job_created", {'handle': handle})
        elif func == "submit_job_high":
            handle = self.manager.add_job(self, high=True, **args)
            self.send_command("job_created", {'handle': handle})
        elif func == "submit_job_bg":
            handle = self.manager.add_job(self, bg=True, **args)
            self.send_command("job_created", {'handle': handle})
        elif func in ("can_do", "can_do_timeout"):
            self.manager.can_do(self, **args)
        elif func == "cant_do":
            self.manager.cant_do(self, **args)
        elif func == "grab_job":
            job = self.manager.grab_job(self)
            if job:
                self.send_command("job_assign", {'handle':job.handle, 'func':job.func, 'arg':job.arg})
            else:
                self.send_command("no_job")
        elif func == "pre_sleep":
            if not self.manager.sleep(self):
                self.wakeup()
        elif func == "work_complete":
            self.manager.work_complete(self, **args)
        elif func == "work_fail":
            self.manager.work_fail(self, **args)
        # Text commands
        elif func == "status":
            status = self.manager.get_status(self)
            for s in status:
                self.send_buffered("%s\t%d\t%d\t%d\n" % (s['func'], s['num_jobs'], s['num_working'], s['num_workers']))
            self.send_buffered(".\n")
        elif func == "version":
            from gearman import __version__
            self.send_buffered("%s\n" % __version__)
        elif func == "workers":
            for client, state in self.manager.states.items():
                # if not state.abilities:
                #     continue
                self.send_buffered("%d %s %s : %s\n" % (client.socket.fileno(), client.addr[0], state.client_id, " ".join(state.abilities)))
            self.send_buffered(".\n")
        # elif func == "maxqueue":
        # 
        #     This sets the maximum queue size for a function. If no size is
        #     given, the default is used. If the size is negative, then the queue
        #     is set to be unlimited. This sends back a single line with "OK".
        # 
        #     Arguments:
        #     - Function name.
        #     - Optional maximum queue size.
        # 
        elif func == "shutdown":
            # TODO: optional "graceful" argument - close listening socket and let all existing connections complete
            self.server.stop()
        else:
            logging.error("Unhandled command %s: %s" % (func, args))

    def handle_write(self):
        if len(self.out_buffer) == 0:
            return 0

        try:
            nsent = self.send(self.out_buffer)
        except socket.error, e:
            self.close()
            return

        self.out_buffer = buffer(self.out_buffer, nsent)

    def send_buffered(self, data):
        self.out_buffer += data

    def send_command(self, name, kwargs={}):
        self.send_buffered(pack_command(name, response=True, **kwargs))

    def wakeup(self):
        self.send_command('noop')

    def work_complete(self, handle, result):
        self.send_command('work_complete', {'handle':handle, 'result':result})

    def work_fail(self, handle):
        self.send_command('work_fail', {'handle':handle})

class Job(object):
    def __init__(self, owner, handle, func, arg, bg=False, high=False, uniq=None):
        self.owner = owner
        self.handle = handle
        self.func = func
        self.arg = arg
        self.bg = bg
        self.high = high
        self.uniq = uniq
        self.worker = None
        self.timeout = None

class ClientState(object):
    def __init__(self, client):
        self.client = client
        self.sleeping = False
        self.client_id = "-"
        # Clients
        self.jobs = []
        # Workers
        self.abilities = {}
        self.working = []

class GearmanTaskManager(object):
    def __init__(self):
        self.max_id = 0
        self.states = {}     # {client: ClientState}
        self.jobqueue = {}   # {function, [job]}
        self.jobs = {}       # {handle: job}
        self.uniq_jobs = {}  # {function: {uniq: job}}
        self.workers = {}    # {function: [state]}
        self.working = set() # set([job])

    def add_job(self, client, func, arg, uniq=None, high=False, bg=False):
        state = self.states[client]
        job = Job(state, self.new_handle(), func=func, arg=arg, uniq=uniq, high=False, bg=False)
        state.jobs.append(job)
        if func not in self.jobqueue:
            self.jobqueue[func] = deque([job])
        else:
            self.jobqueue[func].append(job)
        self.jobs[job.handle] = job
        workers = self.workers.get(func, [])
        for w in workers:
            if w.sleeping:
                w.client.wakeup()
        return job.handle

    def can_do(self, client, func, timeout=None):
        state = self.states[client]
        state.abilities[func] = int(timeout) if timeout else None

        if func not in self.workers:
            self.workers[func] = set((state,))
        else:
            self.workers[func].add(state)

    def cant_do(self, client, func):
        state = self.states[client]
        state.abilities.pop(func, None)
        self.workers[func].pop(state, None)

    def grab_job(self, client, grab=True):
        state = self.states[client]
        abilities = state.abilities.keys()
        random.shuffle(abilities)
        for f in abilities:
            jobs = self.jobqueue.get(f)
            if jobs:
                if not grab:
                    return True

                job = jobs.popleft()
                job.worker = state
                timeout = state.abilities[f]
                job.timeout = time.time() + timeout if timeout else None
                self.working.add(job)
                state.working.append(job)
                return job
                
        return None

    def sleep(self, client):
        has_job = self.grab_job(client, False)
        if has_job:
            return False
        state = self.states[client]
        state.sleeping = True
        return True

    def work_complete(self, client, handle, result):
        job = self.jobs[handle]
        job.owner.client.work_complete(handle, result)
        self._remove_job(job)

    def work_fail(self, client, handle):
        job = self.jobs[handle]
        job.owner.client.work_fail(handle)
        self._remove_job(job)

    def _remove_job(self, job):
        job.owner.jobs.remove(job)
        job.worker.working.remove(job)
        self.working.discard(job)

    def get_status(self, client):
        funcs = set(self.workers.keys()) | set(self.jobqueue.keys())
        status = []
        for f in sorted(funcs):
            workers = self.workers.get(f, [])
            num_workers = len(workers)
            num_working = len(self.working)
            num_jobs = num_working + len(self.jobs.get(f, []))
            status.append(dict(
                func = f,
                num_jobs = num_jobs,
                num_working = num_working,
                num_workers = num_workers,
            ))
        return status

    def check_timeouts(self):
        now = time.time()
        to_fail = []
        for job in self.working:
            if job.timeout and job.timeout < now:
                to_fail.append(job.handle)
        for handle in to_fail:
            self.work_fail(None, handle)

    def register_client(self, client):
        self.states[client] = ClientState(client)

    def deregister_client(self, client):
        state = self.states[client]
        del self.states[client]

        for f in state.abilities:
            self.workers[f].remove(state)

        for j in state.jobs:
            del self.jobs[j.handle]
            self.jobqueue[j.func].remove(j)

    def new_handle(self):
        self.max_id += 1
        return str(self.max_id)

class GearmanServer(asyncore.dispatcher):
    def __init__(self, host="127.0.0.1", port=DEFAULT_PORT):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(5)
        self.manager = GearmanTaskManager()

    def handle_accept(self):
        sock, addr = self.accept()
        GearmanServerClient(sock, addr, self, self.manager)

    def start(self):
        self.running = True
        while self.running:
            asyncore.loop(timeout=1, use_poll=False, count=1)
            self.manager.check_timeouts()

    def stop(self):
        self.running = False
