import collections
import errno
import logging
import socket
import struct
import time
import sys

from gearman._connection import Connection
from gearman.errors import ConnectionError, ProtocolError, ServerUnavailable
from gearman.constants import DEFAULT_PORT, _DEBUG_MODE_
from gearman.protocol import GEARMAN_PARAMS_FOR_COMMAND, GEARMAN_COMMAND_TEXT_COMMAND, NULL_CHAR, \
    get_command_name, pack_binary_command, parse_binary_command, parse_text_command, pack_text_command

gearman_logger = logging.getLogger(__name__)

class GearmanConnection(Connection):
    def __init__(self, host=None, port=None, on_incoming_command=None):
        port = port or DEFAULT_PORT
        super(GearmanConnection, self).__init__(host=host, port=port)

        # On readable command callback takes cmd_type, and a bunch of keyword arguments
        self.on_incoming_command = on_incoming_command

    def handle_read(self):
        """Reads data from socket --> buffer"""
        super(GearmanConnection, self).handle_read()
        
        while True:
            cmd_tuple = self.recv_command()
            if cmd_tuple is None:
                break

            cmd_type, cmd_args = cmd_tuple
            self.on_incoming_command(cmd_type, cmd_args)

    def recv_command(self):
        io_buffer = self.peek()
        cmd_type, cmd_args, cmd_len = self._unpack_command(io_buffer)
        if not cmd_len:
            return None

        self.recv(bufsize=cmd_len)
        return cmd_type, cmd_args

    def send_command(self, cmd_type, cmd_args):
        data_stream = self._pack_command(command_type, command_args)
        self.send(data_stream)

    def _pack_command(self, cmd_type, cmd_args):
        """Converts a command to its raw binary format"""
        if cmd_type not in GEARMAN_PARAMS_FOR_COMMAND:
            raise ProtocolError('Unknown command: %r' % get_command_name(cmd_type))

        if _DEBUG_MODE_:
            gearman_logger.debug('%s - Send - %s - %r', hex(id(self)), get_command_name(cmd_type), cmd_args)

        if cmd_type == GEARMAN_COMMAND_TEXT_COMMAND:
            return pack_text_command(cmd_type, cmd_args)
        else:
            # We'll be sending a response if we know we're a server side command
            is_response = bool(self._is_server_side)
            return pack_binary_command(cmd_type, cmd_args, is_response)

   def _unpack_command(self, given_buffer):
        """Conditionally unpack a binary command or a text based server command"""
        if not given_buffer:
            cmd_type = None
            cmd_args = None
            cmd_len = 0
        elif given_buffer[0] == NULL_CHAR:
            # We'll be expecting a response if we know we're a client side command
            is_response = bool(self._is_client_side)
            cmd_type, cmd_args, cmd_len = parse_binary_command(given_buffer, is_response=is_response)
        else:
            cmd_type, cmd_args, cmd_len = parse_text_command(given_buffer)

        if _DEBUG_MODE_ and cmd_type is not None:
            gearman_logger.debug('%s - Recv - %s - %r', hex(id(self)), get_command_name(cmd_type), cmd_args)

        return cmd_type, cmd_args, cmd_len

    def __repr__(self):
        return ('<GearmanConnection %s:%d state=%s>' % (self._host, self._port, self._state))