#!/usr/bin/env python

import socket

from gearman.connection import DEFAULT_PORT, GearmanConnection

ConnectionError = GearmanConnection.ConnectionError

class GearmanManager(object):
    def __init__(self, server, timeout=5):
        if ':' in server:
            host, port = (server.split(':') + [0])[:2]
            port = int(port) or DEFAULT_PORT
            self.addr = (host, port)
        else:
            self.addr = (server, DEFAULT_PORT)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        try:
            self.sock.connect(self.addr)
        except (socket.error, socket.timeout), exc:
            raise ConnectionError(str(exc))

    def send_command(self, name, reslist=False):
        self.sock.sendall("%s\n" % name)
        buf = ""
        while True:
            buf += self.sock.recv(4096)
            if not reslist:
                if '\n' in buf:
                    return buf.split('\n')[0]
            elif '\n.\n' in buf:
                return buf.split('\n')[:-2]

    def maxqueue(self, func, max_size):
        return self.send_command("maxqueue %s %s" % (func, max_size)) == "OK"

    def status(self):
        status = (s.rsplit('\t', 3) for s in self.send_command("status", True))
        return dict(
            (s[0], {'queued':int(s[1]), 'running':int(s[2]), 'workers':int(s[3])})
            for s in status)

    def shutdown(self, graceful):
        return self.send_command(graceful and "shutdown graceful" or "shutdown") == "OK"

    def version(self):
        return self.send_command("version")

    def workers(self):
        workers = (w.split(' ') for w in self.send_command("workers", True))
        return [
            {
                'fd': int(w[0]),
                'ip': w[1],
                'id': w[2] != '-' and w[2] or None,
                'functions': [x for x in w[4:] if x]
            } for w in workers]
