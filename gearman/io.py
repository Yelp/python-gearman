import select

import gearman.errors
import gearman.util

# epoll event types
_EPOLLIN = 0x01
_EPOLLOUT = 0x04
_EPOLLERR = 0x08
_EPOLLHUP = 0x10

READ = _EPOLLIN
WRITE = _EPOLLOUT
ERROR = _EPOLLERR | _EPOLLHUP

def get_connection_poller():
    """
    Returns a select.epoll-like object. Depending on the platform, this will
    either be:
        - On modern Linux system, with python >= 2.6: select.epoll
        - On all other systems: gearman.io._Select: an object that mimics
          select.epoll, but uses select.select
    """
    if hasattr(select, "epoll"):
        return select.epoll()
    else:
        return _Select()

def _find_bad_connections(connections):
    """
    Find any bad connections in a list of connections. 
    
    For use with select.select. 
    
    When select throws an exception, it's likely that one of the sockets
    passed in has died. In order to find the bad connections, they must be
    checked individually. This will do so and return a list of any bad
    connections found.
    """
    bad = []
    for conn in connections:
        try:
            _, _, _ = gearman.util.select([conn], [], [], timeout=0)
        except (select.error, gearman.errors.ConnectionError):
            bad.append(conn)
    return bad

class _Select(object):
    """
    A `select.epoll`-like object that uses select.select.

    Used as a fallback when epoll is not available. Inspired by tornado's
    fallback mechanism
    """

    def __init__(self):
        self.read = set()
        self.write = set()
        self.error = set()

    def close(self):
        """
        Close the _Select object. For parity with select.epoll. Does nothing
        here.
        """
        pass

    def register(self, fd, evmask):
        """
        Register a file descriptor for polling. 

        fd: a file descriptor (socket) to be registers
        evmask: a bit set describing the desired events to report

        Events are similar to those accepted by select.epoll:
            - gearman.io.READ: report when fd is readable (i.e.: a socket.recv
              operation likely won't block, and will yield some data)
            - gearman.io.WRITE: report when fd is writable (i.e.: a socket.send
              operation likely won't block, and will be able to write some
              data)
            - gearman.io.ERROR: report when fd is in an error state
        """
        if fd in self.read or fd in self.write or fd in self.error:
            raise ValueError("Connection already registered: %d" % fd.fileno())
        if evmask & READ:
            self.read.add(fd)
        if evmask & WRITE:
            self.write.add(fd)
        if evmask & ERROR:
            self.error.add(fd)

    def modify(self, fd, evmask):
        """
        Update the IO events that should be reported for a given file
        descriptor. See _Select.register for details on these events
        """
        self.unregister(fd)
        self.register(fd, evmask)

    def unregister(self, fd):
        """
        Stop tracking events for a given file descriptor
        """
        self.read.discard(fd)
        self.write.discard(fd)
        self.error.discard(fd)

    def poll(self, timeout):
        """
        Wait for events for any of the of register file descriptors. The
        maximum time to wait is specified by the timeout value.

        A timeout < 0 will block indefinitely. A timeout of 0 will not block at
        all. And, a timeout > 0 will block for at most that many seconds. The
        timeout parameter may be a floating point number.
        """
        readable = set()
        writable = set()
        errors = set()

        if timeout is not None and timeout < 0.0:
            # for parity with epoll, negative timeout = block until there
            # is activity
            timeout = None

        connections = (self.read|self.write|self.error)
        
        success = False
        while not success and connections:
            connections -= errors
            try:
                r, w, e = gearman.util.select(self.read, 
                        self.write, self.error, timeout)
                readable = set(r)
                writable = set(w)
                errors |= set(e) #this set could already be populated
                success = True
            except (select.error, gearman.errors.ConnectionError):
                bad_conns = _find_bad_connections(connections)
                map(self.read.discard, bad_conns)
                map(self.write.discard, bad_conns)
                map(self.error.discard, bad_conns)
                errors |= set(bad_conns)
                

        events = {}
        for conn in readable:
            events[conn.fileno()] = events.get(conn.fileno(), 0) | READ
        for conn in writable:
            events[conn.fileno()] = events.get(conn.fileno(), 0) | WRITE
        for conn in errors:
            events[conn.fileno()] = events.get(conn.fileno(), 0) | ERROR

        return events.items()

