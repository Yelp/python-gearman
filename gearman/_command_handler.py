import logging
from gearman.protocol import get_command_name

gearman_logger = logging.getLogger('gearman._command_handler')

class GearmanCommandHandler(object):
    """A command handler manages the state which we should be in given that we received a certain stream of commands"""
    def __init__(self, connection_manager):
        self.connection_manager = connection_manager

    def initial_state(self, *largs, **kwargs):
        pass

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
            raise ValueError(missing_callback_msg)

        # Expand the arguments as passed by the protocol
        # This must match the parameter names as defined in the command handler
        completed_work = cmd_callback(**cmd_args)
        return completed_work

    def recv_error(self, error_code, error_text):
        """When we receive an error from the server, notify the connection manager that we have a gearman error"""
        return self.connection_manager.on_gearman_error(error_code, error_test)
