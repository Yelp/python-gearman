#!/usr/bin/env python
"""
Gearman Client Utils
"""
import select as select_lib
import errno
from gearman.constants import DEFAULT_GEARMAN_PORT, DEFAULT_POLLING_TIMEOUT

def disambiguate_server_parameter(hostport_tuple):
    """Takes either a tuple of (address, port) or a string of 'address:port' and disambiguates them for us"""
    if type(hostport_tuple) is tuple:
        gearman_host, gearman_port = hostport_tuple
    else:
        gearman_host = hostport_tuple
        gearman_port = DEFAULT_GEARMAN_PORT

    return gearman_host, gearman_port

def select(rlist, wlist, xlist, timeout=None):
    """Behave similar to select.select, except ignoring certain types of exceptions"""
    rd_list = []
    wr_list = []
    ex_list = []

    actual_timeout = timeout or DEFAULT_POLLING_TIMEOUT
    try:
        rd_list, wr_list, ex_list = select_lib.select(rlist, wlist, xlist, actual_timeout)
    except select_lib.error, exc:
        # Ignore interrupted system call, reraise anything else
        if exc[0] != errno.EINTR:
            raise

    return rd_list, wr_list, ex_list
