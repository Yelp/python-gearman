import logging
import time

import gearman.util
from gearman.connection import GearmanConnection

gearman_logger = logging.getLogger("gearman._client_base")

class GearmanClientBase(object):
    """Abstract base class for any Gearman-type client that needs to connect/listen to multiple connections
    
    Mananges and polls a group of gearman connections
    
    """
    client_type = None

    def __init__(self, host_list, blocking_timeout=0.0):
        """By default we're going to setup non-blocking connections"""
        assert self.client_type is not None, "Cannot instantiate GearmanClientBase directly as this is an abstract base class"

        self.command_handlers = {}
        self.connection_list = []
        for hostport_tuple in host_list:
            gearman_host, gearman_port = gearman.util.disambiguate_server_parameter(hostport_tuple)
            client_connection = GearmanConnection(gearman_host, gearman_port, blocking_timeout=blocking_timeout)
            self.connection_list.append(client_connection)

    def poll_connections_once(self, connections, timeout=None):
        """Does a single robust select, catching socket errors and doing all handle_* callbacks"""
        all_conns = set(connections)
        dead_conns = set()
        select_conns = all_conns

        rd_list = []
        wr_list = []
        ex_list = []

        gearman_logger.debug("Polling up to %d connection(s)", len(connections))

        successful_select = False
        while not successful_select and select_conns:
            select_conns = all_conns - dead_conns
            rx_conns = [c for c in select_conns if c.readable()]
            tx_conns = [c for c in select_conns if c.writable()]
    
            try:
                rd_list, wr_list, ex_list = gearman.util.select(rx_conns, tx_conns, select_conns, timeout=timeout)
                successful_select = True
            except:
                # On any exception, we're going to assume we ran into a socket exception
                # We'll need to fish for bad connections as suggested at
                #
                # http://www.amk.ca/python/howto/sockets/
                for conn_to_test in select_conns:
                    try:
                        _, _, _ = gearman.util.select([conn_to_test], [], [], timeout=0)
                    except:
                        dead_conns.add(conn_to_test)

        for conn in rd_list:
            self.handle_read(conn)

        for conn in wr_list:
            self.handle_write(conn)

        for conn in ex_list:
            self.handle_error(conn)

        for conn in dead_conns:
            self.handle_error(conn)

        gearman_logger.debug("Read / Write / Error connection(s) - %d / %d / %d", len(rd_list), len(wr_list), len(ex_list) + len(dead_conns))

        return any([rd_list, wr_list, ex_list])

    def poll_connections_until_stopped(self, submitted_connections, stopping_function, timeout=None):
        stop_time = time.time() + (timeout or 0.0)

        continue_working = True
        while continue_working:
            time_remaining = stop_time - time.time()
            polling_timeout = (timeout and time_remaining) or None

            # Keep polling our connections until we find that our request states have all been updated
            any_activity = self.poll_connections_once(submitted_connections, timeout=polling_timeout)

            should_stop = stopping_function(self)
            continue_working = bool(not should_stop) and bool(any_activity)

    def handle_read(self, conn):
        """By default, we'll handle reads by processing out command list and calling the appropriate command handlers"""
        cmd_list = conn.recv_command_list()
        for cmd_tuple in cmd_list:
            continue_working = self.handle_incoming_command(conn, cmd_tuple)
            if continue_working is False:
                break

    def handle_write(self, conn):
        while conn.writable():
            conn.send_data_from_buffer()

    def handle_error(self, conn):
        gearman_logger.error("Exception on connection %s" % conn)
        conn.close()

    def handle_incoming_command(self, conn, cmd_tuple):
        """Convenience method to call after """
        completed_work = None
        if cmd_tuple is None:
            return completed_work

        cmd_type, cmd_args = cmd_tuple
        if cmd_type not in self.command_handlers:
            raise KeyError("Received an unexpected cmd_type: %r not in %r" % (GEARMAN_COMMAND_TO_NAME.get(cmd_type, cmd_type), self.command_handlers.keys()))

        cmd_callback = self.command_handlers[cmd_type]
        completed_work = cmd_callback(conn, cmd_type, cmd_args)
        return completed_work