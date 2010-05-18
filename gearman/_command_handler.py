import logging
import select as select_lib
import time

import gearman.util
from gearman._connection import GearmanConnection
from gearman.errors import ConnectionError
from gearman.protocol import get_command_name

gearman_logger = logging.getLogger('gearman._connection_manager')

class GearmanCommandHandler(object):
    def __init__(self, connection_manager):
        self.connection_manager = connection_manager

        # Initialize the state of this command handler
        self.reset_state()

    def on_connection_error(self):
        pass

    def reset_state(self):
        raise NotImplementedError

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
            missing_callback_msg = 'Could not handle command: %r - %r' % (get_command_name(cmd_type), cmd_args)
            gearman_logger.error(missing_callback_msg)
            raise ValueError(missing_callback_msg)

        # Expand the arguments as parsed from the connection
        # This must match the parameter names as defined in the command handler
        completed_work = cmd_callback(**cmd_args)
        return completed_work

    def recv_error(self, error_code, error_text):
        gearman_logger.error('Error from server: %s: %s' % (error_code, error_text))
        self.connection_manager.on_handler_error(self)

        return False

    # Re-route all IO dealing with the connection through the base_client
    def send_command(self, cmd_type, **cmd_args):
        self.connection_manager.send_command(self, cmd_type, cmd_args)
