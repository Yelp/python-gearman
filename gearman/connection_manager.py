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

        self._connection_poller = self.connection_poller_class()

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
        self._connection_poller.add_connection(current_connection)

        connection_address = current_connection.getpeername()
        self._address_to_connection_map[connection_address] = current_connection

        # Setup this CommandHandler
        current_handler = self.command_handler_class()
        current_handler.set_connection_manager(self)
        current_handler.set_connection(current_connection)
        self._connection_to_handler_map[current_connection] = current_handler

        current_connection.set_command_handler(current_handler)
        self._setup_handler(current_handler)

        return current_connection

    def _setup_handler(self, current_handler):
        return current_handler

    @property
    def connection_list(self):
        return self._connection_to_handler_map.keys()
