import logging
import select as select_lib

import gearman.util
from gearman.connection_poller import GearmanConnectionPoller
from gearman.connection import GearmanConnection
from gearman.constants import _DEBUG_MODE_
from gearman.errors import ConnectionError, ServerUnavailable
from gearman.job import GearmanJob, GearmanJobRequest
from gearman import compat

gearman_logger = logging.getLogger(__name__)

class DataEncoder(object):
    @classmethod
    def encode(cls, encodable_object):
        raise NotImplementedError

    @classmethod
    def decode(cls, decodable_string):
        raise NotImplementedError

class NoopEncoder(DataEncoder):
    """Provide common object dumps for all communications over gearman"""
    @classmethod
    def _enforce_byte_string(cls, given_object):
        if type(given_object) != str:
            raise TypeError("Expecting byte string, got %r" % type(given_object))

    @classmethod
    def encode(cls, encodable_object):
        cls._enforce_byte_string(encodable_object)
        return encodable_object

    @classmethod
    def decode(cls, decodable_string):
        cls._enforce_byte_string(decodable_string)
        return decodable_string

class GearmanConnectionManager(object):
    """Abstract base class for any Gearman-type client that needs to connect/listen to multiple connections

    Mananges and polls a group of gearman connections
    Forwards all communication between a connection and a command handler
    The state of a connection is represented within the command handler

    Automatically encodes all 'data' fields as specified in protocol.py
    """
    command_handler_class = None
    connection_class = GearmanConnection
    connection_poller_class = GearmanConnectionPoller

    job_class = GearmanJob
    job_request_class = GearmanJobRequest

    data_encoder = NoopEncoder

    def __init__(self, host_list=None):
        assert self.command_handler_class is not None, 'GearmanClientBase did not receive a command handler class'

        self._address_to_connection_map = {}

        self._connection_to_handler_map = {}
        self._handler_to_connection_map = {}

        self._connection_poller = self.connection_poller_class()
        self._connected_set = set()

        host_list = host_list or []
        for hostport_tuple in host_list:
            self.connect_to_host(hostport_tuple)

    def shutdown(self):
        # Shutdown all our connections one by one
        for gearman_connection in self._connection_to_handler_map.iterkeys():
            try:
                gearman_connection.close()
            except ConnectionError:
                pass

    ###################################
    # Connection management functions #
    ###################################

    def connect_to_host(self, hostport_tuple):
        """Add a new connection to this connection manager"""
        gearman_host, gearman_port = gearman.util.disambiguate_server_parameter(hostport_tuple)

        current_connection = self.connection_class(host=gearman_host, port=gearman_port)

        # Establish a connection immediately - check for socket exceptions like: "host not found"
        current_connection.connect()

        client_connection = self.register_connection(current_connection)

        return client_connection

    def register_connection(self, current_connection):
        # Once we have a socket, register this connection with the poller
        current_connection.set_connection_manager(self)

        self._connection_poller.add_connection(current_connection)

        connection_address = current_connection.getpeername()
        self._address_to_connection_map[connection_address] = current_connection

        # Setup this CommandHandler
        current_handler = self.command_handler_class(connection_manager=self)
        self._setup_handler(current_handler)

        # Setup bi-directional maps so we can forward events between these two calsses
        self._connection_to_handler_map[current_connection] = current_handler
        self._handler_to_connection_map[current_handler] = current_connection

        return current_connection

    def unregister_connection(self, current_connection):
        # Immediately signal a disconnect!
        current_handler = self.on_disconnect(current_connection)

        self._handler_to_connection_map.pop(current_handler)
        self._connection_to_handler_map.pop(current_connection)
        
        connection_address = current_connection.getpeername()
        self._address_to_connection_map.pop(connection_address)
        
        self._connection_poller.remove_connection(current_connection)

        current_connection.set_connection_manager(None)

        return current_connection

    @property
    def connection_list(self):
        return self._connection_to_handler_map.keys()

    def _setup_handler(self, current_handler):
        return current_handler

    def poll_connections_until_stopped(self, continue_polling_callback, timeout=None):
        return self._connection_poller.poll_until_stopped(continue_polling_callback, timeout=timeout)

    def wait_until_connection_established(self, poll_timeout=None):
        # Poll to make sure we send out our request for a status update
        def continue_while_not_connected():
            self._connected_set.clear()
            for current_connection in self.connection_list:
                if current_connection.connected:
                    self._connected_set.add(current_connection)
                elif not current_connection.connecting:
                    self.attempt_connect(current_connection)

            return not bool(self._connected_set)

        self.poll_connections_until_stopped(continue_while_not_connected, timeout=poll_timeout)

    def attempt_connect(self, current_connection):
        current_connection.connect()
 
    ###################################
    # Connection management functions #
    ###################################

    ### Handlers to take care of Connection Events
    def on_recv_command(self, current_connection, cmd_type, cmd_args):
        """Forwarding connection communication on to the respect handler"""
        current_handler = self._connection_to_handler_map[current_connection]
        current_handler.recv_command(cmd_type, cmd_args)
        return current_handler

    def on_connect(self, current_connection):
        current_handler = self._connection_to_handler_map[current_connection]
        current_handler.on_connect()
        return current_handler

    def on_disconnect(self, current_connection):
        current_handler = self._connection_to_handler_map[current_connection]
        current_handler.on_disconnect()
        return current_handler

    # Handlers to take care of CommandHandlerEvents
    def on_send_command(self, current_handler, cmd_type, cmd_args):
        current_connection = self._handler_to_connection_map[current_handler]
        current_connection.send_command(cmd_type, cmd_args)
        return current_connection

    def on_gearman_error(self, current_handler):
        current_connection = self._handler_to_connection_map[current_handler]
        return current_connection
