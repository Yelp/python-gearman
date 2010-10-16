import logging
import select as select_lib

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

        self._polling = False
        self._stopwatch = None

    def add_connection(self, current_connection):
        """Add a new connection to this connection manager"""
        connection_fd = current_connection.fileno()
        self._fd_to_connection_map[connection_fd] = current_connection

    def remove_connection(self, current_connection):
        connection_fd = current_connection.fileno()
        self._fd_to_connection_map.pop(connection_fd)

    def poll_fds_until_stopped(self, submitted_fds, continue_polling_callback, timeout=None):
        """Continue to poll our connections until we receive a stopping condition"""
        submitted_connections = set(self._fd_to_connection_map[current_fd] for current_fd in submitted_fds)
        return self.poll_connections_until_stopped(submitted_connections, continue_polling_callback, timeout=timeout)

    def poll_connections_until_stopped(self, submitted_connections, continue_polling_callback, timeout=None):
        stopwatch = gearman.util.Stopwatch(timeout)

        continue_polling = True
        while continue_polling:
            time_remaining = self._stopwatch.get_time_remaining()

            # Do a single robust select and handle all connection activity
            read_connections, write_connections, dead_connections = self._poll_connections_once(submitted_connections, timeout=time_remaining)
            self._handle_connection_activity(read_connections, write_connections, dead_connections)

            connection_ok = compat.any(bool(not current_connection.disconnected) for current_connection in submitted_connections)
            stopwatch_ok = bool(time_remaining != 0.0)
            callback_ok = continue_polling_callback()

            continue_polling = compat.all([connection_ok, stopwatch_ok, callback_ok, self._polling])

    def _poll_connections_once(self, submitted_connections, timeout=None):
        """Does a single robust select, catching socket errors"""
        select_connections = set(submitted_connections)

        rd_connections = set()
        wr_connections = set()
        ex_connections = set()

        if timeout is not None and timeout < 0.0:
            return rd_connections, wr_connections, ex_connections

        successful_select = False
        while not successful_select and select_connections:
            select_connections -= ex_connections
            check_rd_connections = [current_connection for current_connection in select_connections if current_connection.readable()]
            check_wr_connections = [current_connection for current_connection in select_connections if current_connection.writable()]

            try:
                rd_list, wr_list, ex_list = gearman.util.select(check_rd_connections, check_wr_connections, select_connections, timeout=timeout)
                rd_connections |= set(rd_list)
                wr_connections |= set(wr_list)
                ex_connections |= set(ex_list)

                successful_select = True
            except (select_lib.error, ConnectionError):
                # On any exception, we're going to assume we ran into a socket exception
                # We'll need to fish for bad connections as suggested at
                #
                # http://docs.python.org/howto/sockets
                for conn_to_test in select_connections:
                    try:
                        _, _, _ = gearman.util.select([conn_to_test], [], [], timeout=0.0)
                    except (select_lib.error, ConnectionError):
                        rd_connections.discard(conn_to_test)
                        wr_connections.discard(conn_to_test)
                        ex_connections.add(conn_to_test)

                        gearman_logger.error('select error: %r' % conn_to_test)

        if _DEBUG_MODE_:
            gearman_logger.debug('select :: Poll - %d :: Read - %d :: Write - %d :: Error - %d', \
                len(select_connections), len(rd_connections), len(wr_connections), len(ex_connections))

        return rd_connections, wr_connections, ex_connections

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
