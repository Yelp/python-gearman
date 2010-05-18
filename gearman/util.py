#!/usr/bin/env python
"""
Gearman Client Utils
"""
import select as select_lib
import errno
from gearman.constants import DEFAULT_GEARMAN_PORT

def unlist(given_list):
	"""Convert the (possibly) single item list into a single item"""
	list_size = len(given_list)
	if list_size > 1:
		raise ValueError(list_size)
	elif list_size == 0:
		return None
	else:
		return given_list[0]

def disambiguate_server_parameter(hostport_tuple):
    """Takes either a tuple of (address, port) or a string of 'address:port' and disambiguates them for us"""
    if type(hostport_tuple) is tuple:
        gearman_host, gearman_port = hostport_tuple
    elif ':' in hostport_tuple:
        gearman_host, gearman_possible_port = hostport_tuple.split(':')
        gearman_port = int(gearman_possible_port)
    else:
        gearman_host = hostport_tuple
        gearman_port = DEFAULT_GEARMAN_PORT

    return gearman_host, gearman_port

def select(rlist, wlist, xlist, timeout=None):
    """Behave similar to select.select, except ignoring certain types of exceptions"""
    rd_list = []
    wr_list = []
    ex_list = []

    select_args = [rlist, wlist, xlist]
    if timeout is not None:
        select_args.append(timeout)

    try:
        rd_list, wr_list, ex_list = select_lib.select(*select_args)
    except select_lib.error, exc:
        # Ignore interrupted system call, reraise anything else
        if exc[0] != errno.EINTR:
            raise

    return rd_list, wr_list, ex_list
