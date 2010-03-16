#!/usr/bin/env python
# TODO: Update the GearmanMananger to use GearmanConnection

import socket

from gearman.connection import DEFAULT_GEARMAN_PORT, GearmanConnection
from gearman.errors import ConnectionError
from gearman.protocol import *

class GearmanManager(object):
    def __init__(self, server, timeout=5):
        if ':' in server:
            host, port = (server.split(':') + [0])[:2]
            port = int(port) or DEFAULT_GEARMAN_PORT
            self.addr = (host, port)
        else:
            self.addr = (server, DEFAULT_GEARMAN_PORT)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        try:
            self.sock.connect(self.addr)
        except (socket.error, socket.timeout), exc:
            raise ConnectionError(str(exc))

    def send_command(self, cmd_name, expecting_results_list=False):
        self.sock.sendall("%s\n" % cmd_name)
        buf = ""
        while True:
            buf += self.sock.recv(4096)
            if not expecting_results_list:
                if '\n' in buf:
                    return buf.split('\n')[0]
            elif '\n.\n' in buf:
                return buf.split('\n')[:-2]

    def maxqueue(self, func, max_size):
        cmd_resp = self.send_command("%s %s %s" % (GEARMAN_SERVER_COMMAND_MAXQUEUE, func, max_size))
        return bool(cmd_resp == "OK")

    def status(self):
        cmd_resp = self.send_command(GEARMAN_SERVER_COMMAND_STATUS, expecting_results_list=True)
        status = (s.rsplit('\t', 3) for s in cmd_resp)
        return dict(
            (s[0], {'queued':int(s[1]), 'running':int(s[2]), 'workers':int(s[3])})
            for s in status)

    def shutdown(self, graceful=True):
        actual_command = GEARMAN_SERVER_COMMAND_SHUTDOWN
        if graceful:
            actual_command += " graceful"

        cmd_resp = self.send_command(actual_command)
        return bool(cmd_resp == "OK")

    def version(self):
        return self.send_command(GEARMAN_SERVER_COMMAND_VERSION)

    def workers(self):
        cmd_resp = self.send_command(GEARMAN_SERVER_COMMAND_WORKERS, expecting_results_list=True)
        workers = [w.split(' ') for w in cmd_resp]
        return [
            {
                'fd': int(w[0]),
                'ip': w[1],
                'id': w[2] != '-' and w[2] or None,
                'functions': [x for x in w[4:] if x]
            } for w in workers]
