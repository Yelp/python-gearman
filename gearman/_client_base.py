import collections
import logging
import select as select_lib
import time

import gearman.util
from gearman.connection import GearmanConnection
from gearman.errors import ConnectionError
from gearman.protocol import get_command_name

gearman_logger = logging.getLogger('gearman._client_base')

class GearmanClientBase(object):
    """Abstract base class for any Gearman-type client that needs to connect/listen to multiple connections

    Mananges and polls a group of gearman connections

    """
    def __init__(self, host_list=None, blocking_timeout=0.0, gearman_connection_handler_class=None, gearman_connection_class=None):
        """By default we're going to setup non-blocking connections"""
        self.connection_handlers = {}
        self.connection_list = []

        gearman_connection_class = gearman_connection_class or GearmanConnection
        assert gearman_connection_handler_class is not None, 'GearmanClientBase did not receive a connection handler class'
        self.gearman_connection_handler_class = gearman_connection_handler_class

        host_list = host_list or []
        for hostport_tuple in host_list:
            gearman_host, gearman_port = gearman.util.disambiguate_server_parameter(hostport_tuple)
            client_connection = gearman_connection_class(gearman_host, gearman_port, blocking_timeout=blocking_timeout)

            self._add_connection(client_connection)

    def _add_connection(self, client_connection):
        # Create connection handler for every connection
        self.connection_handlers[client_connection] = self.gearman_connection_handler_class(client_base=self, connection=client_connection)
        self.connection_list.append(client_connection)

    ##################################
    # Callbacks for Command Handlers #
    ##################################

    def _check_connection_association(self, gearman_connection):
        if gearman_connection not in self.connection_list:
            raise ValueError('Given connection not being managed by this client: %r' % gearman_connection)

    def send_command(self, gearman_connection, cmd_type, cmd_args):
        self._check_connection_association(gearman_connection)
        if not gearman_connection.is_connected():
            raise ConnectionError('Attempted to send a command on a dead connection: %r - %r - %r' % (gearman_connection, get_command_name(cmd_type), cmd_args))

        gearman_connection.send_command(cmd_type, cmd_args)

    ##################################
    # Callbacks for Command Handlers #
    ##################################

    def poll_connections_once(self, connections, timeout=None):
        """Does a single robust select, catching socket errors and doing all handle_* callbacks"""
        all_conns = set(connections)
        dead_conns = set()
        select_conns = all_conns

        rd_list = []
        wr_list = []
        ex_list = []

        gearman_logger.debug('Polling up to %d connection(s)', len(connections))
        if timeout is not None and timeout < 0.0:
            return False

        successful_select = False
        while not successful_select and select_conns:
            select_conns = all_conns - dead_conns
            rx_conns = [c for c in select_conns if c.readable()]
            tx_conns = [c for c in select_conns if c.writable()]

            try:
                rd_list, wr_list, ex_list = gearman.util.select(rx_conns, tx_conns, select_conns, timeout=timeout)
                successful_select = True
            except select_lib.error:
                # On any exception, we're going to assume we ran into a socket exception
                # We'll need to fish for bad connections as suggested at
                #
                # http://www.amk.ca/python/howto/sockets/
                for conn_to_test in select_conns:
                    try:
                        _, _, _ = gearman.util.select([conn_to_test], [], [], timeout=0)
                    except select_lib.error:
                        dead_conns.add(conn_to_test)

        for conn in rd_list:
            try:
                self.handle_read(conn)
            except ConnectionError:
                dead_conns.add(conn)

        for conn in wr_list:
            try:
                self.handle_write(conn)
            except ConnectionError:
                dead_conns.add(conn)

        for conn in ex_list:
            self.handle_error(conn)

        for conn in dead_conns:
            self.handle_error(conn)

        gearman_logger.debug('Read / Write / Error connection(s) - %d / %d / %d', len(rd_list), len(wr_list), len(ex_list) + len(dead_conns))

        return any([rd_list, wr_list, ex_list])

    def poll_connections_until_stopped(self, submitted_connections, polling_callback_fxn, timeout=None):
        """Continue to poll our connections until we receive a stopping condition"""
        stop_time = time.time() + (timeout or 0.0)

        any_activity = False
        continue_working = polling_callback_fxn(self, any_activity)
        while continue_working:
            time_remaining = stop_time - time.time()
            has_time_remaining = bool(timeout is None) or bool(time_remaining > 0.0)
            if not has_time_remaining:
                break

            # Keep polling our connections until we find that our request states have all been updated
            polling_timeout = (timeout and time_remaining) or None
            any_activity = self.poll_connections_once(submitted_connections, timeout=polling_timeout)

            continue_working = polling_callback_fxn(self, any_activity)

    def handle_read(self, conn):
        """By default, we'll handle reads by processing out command list and calling the appropriate command handlers"""
        connection_handler = self.connection_handlers.get(conn)
        assert connection_handler, 'Connection handler not found for connection %r' % conn

        cmd_list = conn.recv_command_list()
        for cmd_tuple in cmd_list:
            if cmd_tuple is None:
                continue

            cmd_type, cmd_args = cmd_tuple
            continue_working = connection_handler.recv_command(cmd_type, **cmd_args)
            if continue_working is False:
                break

    def handle_write(self, conn):
        while conn.writable():
            conn.send_data_from_buffer()

    def handle_error(self, conn):
        gearman_logger.error('Exception on connection %s' % conn)
        conn.close()

class GearmanConnectionHandler(object):
    def __init__(self, client_base, connection):
        # assert isinstance(client_base, GearmanClientBase), 'Expecting a class that is an instance of GearmanClientBase'
        self.client_base = client_base
        self.gearman_connection = connection

    def recv_command(self, cmd_type, **cmd_args):
        """Maps any command to a recv_* callback function"""
        completed_work = None

        gearman_command_name = get_command_name(cmd_type)
        if bool(gearman_command_name == cmd_type) or not gearman_command_name.startswith('GEARMAN_COMMAND_'):
            gearman_logger.error('Could not handle command: %r - %r' % (cmd_type, cmd_args))
            raise ValueError('Could not handle command: %r - %r' % (cmd_type, cmd_args))

        recv_command_function_name = gearman_command_name.lower().replace('gearman_command_', 'recv_')

        cmd_callback = getattr(self, recv_command_function_name, None)
        if not cmd_callback:
            gearman_logger.error('Could not handle command: %r - %r' % (cmd_type, cmd_args))
            raise ValueError('Could not handle command: %r - %r' % (cmd_type, cmd_args))

        # Expand the arguments as parsed from the connection
        # This must match the parameter names as defined in the command handler
        completed_work = cmd_callback(**cmd_args)
        return completed_work

    # Re-route all IO dealing with the connection through the base_client
    def send_command(self, cmd_type, **cmd_args):
        self.client_base.send_command(self.gearman_connection, cmd_type, cmd_args)
