import logging
from gearman.errors import UnknownCommandError
from gearman.protocol import get_command_name

gearman_logger = logging.getLogger(__name__)

class GearmanCommandHandler(object):
    """A command handler manages the state which we should be in given a certain stream of commands

    GearmanCommandHandler does no I/O and only understands sending/receiving commands
    """
    def __init__(self, connection_manager=None):
        self.connection_manager = connection_manager

    def initial_state(self, *largs, **kwargs):
        """Called by a Connection Manager after we've been instantiated and we're ready to send off commands"""
        pass

    def on_io_error(self):
        pass

    def decode_data(self, data):
        """Convenience function :: handle binary string -> object unpacking"""
        return self.connection_manager.data_encoder.decode(data)

    def encode_data(self, data):
        """Convenience function :: handle object -> binary string packing"""
        return self.connection_manager.data_encoder.encode(data)

    def fetch_commands(self):
        """Called by a Connection Manager to notify us that we have pending commands"""
        continue_working = True
        while continue_working:
            cmd_tuple = self.connection_manager.read_command(self)
            if cmd_tuple is None:
                break

            cmd_type, cmd_args = cmd_tuple
            continue_working = self.recv_command(cmd_type, **cmd_args)

    def send_command(self, cmd_type, **cmd_args):
        """Hand off I/O to the connection mananger"""
        self.connection_manager.send_command(self, cmd_type, cmd_args)

    def recv_command(self, cmd_type, **cmd_args):
        """Maps any command to a recv_* callback function"""
        completed_work = None

        gearman_command_name = get_command_name(cmd_type)
        if bool(gearman_command_name == cmd_type) or not gearman_command_name.startswith('GEARMAN_COMMAND_'):
            unknown_command_msg = 'Could not handle command: %r - %r' % (gearman_command_name, cmd_args)
            gearman_logger.error(unknown_command_msg)
            raise ValueError(unknown_command_msg)

        recv_command_function_name = gearman_command_name.lower().replace('gearman_command_', 'recv_')

        cmd_callback = getattr(self, recv_command_function_name, None)
        if not cmd_callback:
            missing_callback_msg = 'Could not handle command: %r - %r' % (get_command_name(cmd_type), cmd_args)
            gearman_logger.error(missing_callback_msg)
            raise UnknownCommandError(missing_callback_msg)

        # Expand the arguments as passed by the protocol
        # This must match the parameter names as defined in the command handler
        completed_work = cmd_callback(**cmd_args)
        return completed_work

    def recv_error(self, error_code, error_text):
        """When we receive an error from the server, notify the connection manager that we have a gearman error"""
        return self.connection_manager.on_gearman_error(error_code, error_text)
