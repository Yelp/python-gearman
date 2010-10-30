import logging
import select

import gearman.util
from gearman.connection import GearmanConnection
from gearman.constants import _DEBUG_MODE_
from gearman.errors import ConnectionError, ServerUnavailable
from gearman.job import GearmanJob, GearmanJobRequest
from gearman import compat

gearman_logger = logging.getLogger(__name__)

class GearmanConnectionPoller(object):
    """Abstract base class for any Gearman-type client that needs to connect/listen to multiple connections

    Mananges and polls a group of gearman connections
    Forwards all communication between a connection and a command handler
    The state of a connection is represented within the command handler

    Automatically encodes all 'data' fields as specified in protocol.py
    """
    ###################################
    # Connection management functions #
    ###################################

    def __init__(self):
        self._fd_to_connection_map = {}
        self._connection_set = set()

        self._polling = False

    def add_connection(self, current_connection):
        """Add a new connection to this connection manager"""
        self._connection_set.add(current_connection)

        connection_fd = current_connection.fileno()
        self._fd_to_connection_map[connection_fd] = current_connection

    def remove_connection(self, current_connection):
        self._connection_set.discard(current_connection, None)

        connection_fd = current_connection.fileno()
        self._fd_to_connection_map.pop(connection_fd)

    def poll_until_stopped(self, continue_polling_callback, timeout=None):
        countdown_timer = gearman.util.CountdownTimer(timeout)

        callback_ok = continue_polling_callback()
        timer_ok = bool(not countdown_timer.expired)
        continue_polling = compat.all([callback_ok, timer_ok])

        while continue_polling:
            # Do a single robust select and handle all connection activity
            read_connections, write_connections, dead_connections = self._poll_once(timeout=countdown_timer.time_remaining)
            self._handle_connection_activity(read_connections, write_connections, dead_connections)

            callback_ok = continue_polling_callback()
            connection_ok = compat.any(bool(not current_connection.disconnected) for current_connection in self._connection_set)
            timer_ok = bool(not countdown_timer.expired)

            continue_polling = compat.all([callback_ok, connection_ok, timer_ok])

        # Return True, if we were stopped by our callback
        return bool(not callback_ok)

    def _poll_once(self, timeout=None):
        """Does a single robust select, catching socket errors"""
        connections_to_check = set(self._connection_set)

        actual_rd_conns = set()
        actual_wr_conns = set()
        actual_ex_conns = set()

        if timeout is not None and timeout < 0.0:
            return actual_rd_conns, actual_wr_conns, actual_ex_conns

        successful_select = False
        while not successful_select and connections_to_check:
            connections_to_check -= actual_ex_conns

            check_rd_conns = set(conn for conn in connections_to_check if conn.readable())
            check_wr_conns = set(conn for conn in connections_to_check if conn.writable())
            check_ex_conns = set(connections_to_check)

            try:
                event_rd_conns, event_wr_conns, event_ex_conns = self._execute_select(check_rd_conns, check_wr_conns, check_ex_conns, timeout=timeout)
                actual_rd_conns |= set(event_rd_conns)
                actual_wr_conns |= set(event_wr_conns)
                actual_ex_conns |= set(event_ex_conns)

                successful_select = True
            except (select.error, ConnectionError):
                # On any exception, we're going to assume we ran into a socket exception
                # We'll need to fish for bad connections as suggested at
                #
                # http://docs.python.org/howto/sockets
                for conn_to_test in check_all_connections:
                    try:
                        _, _, _ = self._execute_select([conn_to_test], [], [], timeout=0.0)
                    except (select.error, ConnectionError):
                        actual_rd_conns.discard(conn_to_test)
                        actual_wr_conns.discard(conn_to_test)
                        actual_ex_conns.add(conn_to_test)
                        gearman_logger.error('select error: %r' % conn_to_test)

        if _DEBUG_MODE_:
            gearman_logger.debug('select :: Poll - %d :: Read - %d :: Write - %d :: Error - %d', \
                len(self._connection_set), len(actual_rd_conns), len(actual_wr_conns), len(actual_ex_conns))

        return actual_rd_conns, actual_wr_conns, actual_ex_conns

    def _execute_select(self, rd_conns, wr_conns, ex_conns, timeout=None):
        """Behave similar to select.select, except ignoring certain types of exceptions"""
        select_args = [rd_conns, wr_conns, ex_conns]
        if timeout is not None:
            select_args.append(timeout)

        try:
            rd_list, wr_list, er_list = select.select(*select_args)
        except select.error, exc:
            # Ignore interrupted system call, reraise anything else
            if exc[0] != errno.EINTR:
                raise

	        rd_list = []
	        wr_list = []
	        er_list = []

        return rd_list, wr_list, er_list

    def _handle_connection_activity(self, rd_connections, wr_connections, ex_connections):
        """Process all connection activity... executes all handle_* callbacks"""
        dead_connections = set()
        for current_connection in rd_connections:
            try:
                current_connection.handle_read()
            except ConnectionError:
                dead_connections.add(current_connection)

        for current_connection in wr_connections:
            try:
                current_connection.handle_write()
            except ConnectionError:
                dead_connections.add(current_connection)

        for current_connection in ex_connections:
            current_connection.handle_error()

        for current_connection in dead_connections:
            current_connection.handle_error()

        failed_connections = ex_connections | dead_connections
        return rd_connections, wr_connections, failed_connections
