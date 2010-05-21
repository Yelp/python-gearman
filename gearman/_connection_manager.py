import logging
import select as select_lib
import time

import gearman.util
from gearman._connection import GearmanConnection
from gearman.errors import ConnectionError
from gearman.protocol import get_command_name

gearman_logger = logging.getLogger('gearman._connection_manager')

class GearmanConnectionManager(object):
    """Abstract base class for any Gearman-type client that needs to connect/listen to multiple connections

    Mananges and polls a group of gearman connections
    Forwards commands from the connection to its designated command handler
    """
    command_handler_class = None
    connection_class = GearmanConnection

    def __init__(self, host_list=None, blocking_timeout=0.0):
        """By default we're going to setup non-blocking connections"""
        assert self.command_handler_class is not None, 'GearmanClientBase did not receive a command handler class'

        self.connection_list = []
        self.blocking_timeout = blocking_timeout

        host_list = host_list or []
        for hostport_tuple in host_list:
            self.add_connection(hostport_tuple)

        self.handler_to_connection_map = {}
        self.connection_to_handler_map = {}

        self.handler_initial_state = {}

    def shutdown(self):
        # Shutdown all our connections one by one
        for gearman_connection in self.connection_list:
            gearman_connection.close()

    ###################################
    # Connection management functions #
    ###################################

    def add_connection(self, hostport_tuple):
        """Add a connection"""
        gearman_host, gearman_port = gearman.util.disambiguate_server_parameter(hostport_tuple)

        client_connection = self.connection_class(host=gearman_host, port=gearman_port, blocking_timeout=self.blocking_timeout)
        self.connection_list.append(client_connection)

        return client_connection

    def attempt_connect(self, current_connection):
        """Attempt to connect... if not previous connected, create a new CommandHandler to manage state"""
        assert current_connection in self.connection_list, "Unknown connection - %r" % current_connection
        was_connected = current_connection.is_connected()

        try:
            current_connection.connect()
        except ConnectionError:
            return None

        if not was_connected:
            # Initiate a new command handler every time we start a new connection
            current_handler = self.command_handler_class(connection_manager=self)

            # Handler to connection map for CommandHandler -> Connection interactions
            # Connection to handler map for Connection -> CommandHandler interactions
            self.handler_to_connection_map[current_handler] = current_connection
            self.connection_to_handler_map[current_connection] = current_handler

            current_handler.initial_state(**self.handler_initial_state)

        return current_connection

    def poll_connections_once(self, connections, timeout=None):
        """Does a single robust select, catching socket errors and doing all handle_* callbacks"""
        all_conns = set(connections)
        dead_conns = set()
        select_conns = all_conns

        rd_list = []
        wr_list = []
        ex_list = []

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
            except (select_lib.error, ConnectionError):
                # On any exception, we're going to assume we ran into a socket exception
                # We'll need to fish for bad connections as suggested at
                #
                # http://www.amk.ca/python/howto/sockets/
                for conn_to_test in select_conns:
                    try:
                        _, _, _ = gearman.util.select([conn_to_test], [], [], timeout=0)
                    except (select_lib.error, ConnectionError):
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

        gearman_logger.debug('select :: Poll - %d :: Read - %d :: Write - %d :: Error - %d', len(connections), len(rd_list), len(wr_list), len(ex_list) + len(dead_conns))

        return any([rd_list, wr_list, ex_list])

    def poll_connections_until_stopped(self, submitted_connections, callback_fxn, callback_data=None, timeout=None):
        """Continue to poll our connections until we receive a stopping condition"""
        if not submitted_connections:
            return False

        stopwatch = gearman.util.Stopwatch(timeout)

        any_activity = False
        continue_working = callback_fxn(any_activity, callback_data)
        while continue_working:
            time_remaining = stopwatch.get_time_remaining()
            if time_remaining == 0.0:
                break

            # Keep polling our connections until we find that our request states have all been updated
            any_activity = self.poll_connections_once(submitted_connections, timeout=time_remaining)
            continue_working = callback_fxn(any_activity, callback_data)

        return continue_working

    def handle_read(self, current_connection):
        """By default, we'll handle reads by processing out command list and calling the appropriate command handlers"""
        current_handler = self.connection_to_handler_map[current_connection]

        # Transfer data from socket -> buffer
        current_connection.read_data_from_socket()

        # Transfer command from buffer -> command queue
        current_connection.read_commands_from_buffer()

        continue_working = True
        while continue_working:
            cmd_tuple = current_connection.read_command()
            if cmd_tuple is None:
                break

            cmd_type, cmd_args = cmd_tuple
            continue_working = current_handler.recv_command(cmd_type, **cmd_args)

    def handle_write(self, current_connection):
        # Transfer command from command queue -> buffer
        current_connection.send_commands_to_buffer()

        # Transfer data from buffer -> socket
        current_connection.send_data_to_socket()

    def handle_error(self, current_connection):
        dead_handler    = self.connection_to_handler_map.pop(current_connection, None)
        dead_connection = self.handler_to_connection_map.pop(dead_handler, None)

        current_connection.close()

    ##################################
    # Callbacks for Command Handlers #
    ##################################

    def send_command(self, command_handler, cmd_type, cmd_args):
        """Forward this command handler's request to send a command
        Isolates CommandHandlers from having to know about IO"""
        gearman_connection = self.handler_to_connection_map[command_handler]
        gearman_connection.send_command(cmd_type, cmd_args)

    def on_gearman_error(self, error_code, error_text):
        gearman_logger.error('Received error from server: %s: %s' % (error_code, error_text))
        return False