import struct
from gearman.constants import PRIORITY_NONE, PRIORITY_LOW, PRIORITY_HIGH
from gearman.errors import ProtocolError
from gearman import compat
# Protocol specific constants
NULL_CHAR = '\x00'
MAGIC_RES_STRING = '%sRES' % NULL_CHAR
MAGIC_REQ_STRING = '%sREQ' % NULL_CHAR

COMMAND_HEADER_SIZE = 12

# Gearman commands 1-9
GEARMAN_COMMAND_CAN_DO = 1
GEARMAN_COMMAND_CANT_DO = 2
GEARMAN_COMMAND_RESET_ABILITIES = 3
GEARMAN_COMMAND_PRE_SLEEP = 4
GEARMAN_COMMAND_NOOP = 6
GEARMAN_COMMAND_SUBMIT_JOB = 7
GEARMAN_COMMAND_JOB_CREATED = 8
GEARMAN_COMMAND_GRAB_JOB = 9

# Gearman commands 10-19
GEARMAN_COMMAND_NO_JOB = 10
GEARMAN_COMMAND_JOB_ASSIGN = 11
GEARMAN_COMMAND_WORK_STATUS = 12
GEARMAN_COMMAND_WORK_COMPLETE = 13
GEARMAN_COMMAND_WORK_FAIL = 14
GEARMAN_COMMAND_GET_STATUS = 15
GEARMAN_COMMAND_ECHO_REQ = 16
GEARMAN_COMMAND_ECHO_RES = 17
GEARMAN_COMMAND_SUBMIT_JOB_BG = 18
GEARMAN_COMMAND_ERROR = 19

# Gearman commands 20-29
GEARMAN_COMMAND_STATUS_RES = 20
GEARMAN_COMMAND_SUBMIT_JOB_HIGH = 21
GEARMAN_COMMAND_SET_CLIENT_ID = 22
GEARMAN_COMMAND_CAN_DO_TIMEOUT = 23
GEARMAN_COMMAND_ALL_YOURS = 24
GEARMAN_COMMAND_WORK_EXCEPTION = 25
GEARMAN_COMMAND_OPTION_REQ = 26
GEARMAN_COMMAND_OPTION_RES = 27
GEARMAN_COMMAND_WORK_DATA = 28
GEARMAN_COMMAND_WORK_WARNING = 29

# Gearman commands 30-39
GEARMAN_COMMAND_GRAB_JOB_UNIQ = 30
GEARMAN_COMMAND_JOB_ASSIGN_UNIQ = 31
GEARMAN_COMMAND_SUBMIT_JOB_HIGH_BG = 32
GEARMAN_COMMAND_SUBMIT_JOB_LOW = 33
GEARMAN_COMMAND_SUBMIT_JOB_LOW_BG = 34

# Fake command code 
GEARMAN_COMMAND_TEXT_COMMAND = 9999

GEARMAN_PARAMS_FOR_COMMAND = {
    # Gearman commands 1-9
    GEARMAN_COMMAND_CAN_DO: ['task'],
    GEARMAN_COMMAND_CANT_DO: ['task'],
    GEARMAN_COMMAND_RESET_ABILITIES: [],
    GEARMAN_COMMAND_PRE_SLEEP: [],
    GEARMAN_COMMAND_NOOP: [],
    GEARMAN_COMMAND_SUBMIT_JOB: ['task', 'unique', 'data'],
    GEARMAN_COMMAND_JOB_CREATED: ['job_handle'],
    GEARMAN_COMMAND_GRAB_JOB: [],

    # Gearman commands 10-19
    GEARMAN_COMMAND_NO_JOB: [],
    GEARMAN_COMMAND_JOB_ASSIGN: ['job_handle', 'task', 'data'],
    GEARMAN_COMMAND_WORK_STATUS: ['job_handle', 'numerator', 'denominator'],
    GEARMAN_COMMAND_WORK_COMPLETE: ['job_handle', 'data'],
    GEARMAN_COMMAND_WORK_FAIL: ['job_handle'],
    GEARMAN_COMMAND_GET_STATUS: ['job_handle'],
    GEARMAN_COMMAND_ECHO_REQ: ['data'],
    GEARMAN_COMMAND_ECHO_RES: ['data'],
    GEARMAN_COMMAND_SUBMIT_JOB_BG: ['task', 'unique', 'data'],
    GEARMAN_COMMAND_ERROR: ['error_code', 'error_text'],

    # Gearman commands 20-29
    GEARMAN_COMMAND_STATUS_RES: ['job_handle', 'known', 'running', 'numerator', 'denominator'],
    GEARMAN_COMMAND_SUBMIT_JOB_HIGH: ['task', 'unique', 'data'],
    GEARMAN_COMMAND_SET_CLIENT_ID: ['client_id'],
    GEARMAN_COMMAND_CAN_DO_TIMEOUT: ['task', 'timeout'],
    GEARMAN_COMMAND_ALL_YOURS: [],
    GEARMAN_COMMAND_WORK_EXCEPTION: ['job_handle', 'data'],
    GEARMAN_COMMAND_OPTION_REQ: ['option_name'],
    GEARMAN_COMMAND_OPTION_RES: ['option_name'],
    GEARMAN_COMMAND_WORK_DATA: ['job_handle', 'data'],
    GEARMAN_COMMAND_WORK_WARNING: ['job_handle', 'data'],

    # Gearman commands 30-39
    GEARMAN_COMMAND_GRAB_JOB_UNIQ: [],
    GEARMAN_COMMAND_JOB_ASSIGN_UNIQ: ['job_handle', 'task', 'unique', 'data'],
    GEARMAN_COMMAND_SUBMIT_JOB_HIGH_BG: ['task', 'unique', 'data'],
    GEARMAN_COMMAND_SUBMIT_JOB_LOW: ['task', 'unique', 'data'],
    GEARMAN_COMMAND_SUBMIT_JOB_LOW_BG: ['task', 'unique', 'data'],

    # Fake gearman command
    GEARMAN_COMMAND_TEXT_COMMAND: ['raw_text']
}

GEARMAN_COMMAND_TO_NAME = {
    GEARMAN_COMMAND_CAN_DO: 'GEARMAN_COMMAND_CAN_DO',
    GEARMAN_COMMAND_CANT_DO: 'GEARMAN_COMMAND_CANT_DO',
    GEARMAN_COMMAND_RESET_ABILITIES: 'GEARMAN_COMMAND_RESET_ABILITIES',
    GEARMAN_COMMAND_PRE_SLEEP: 'GEARMAN_COMMAND_PRE_SLEEP',
    GEARMAN_COMMAND_NOOP: 'GEARMAN_COMMAND_NOOP',
    GEARMAN_COMMAND_SUBMIT_JOB: 'GEARMAN_COMMAND_SUBMIT_JOB',
    GEARMAN_COMMAND_JOB_CREATED: 'GEARMAN_COMMAND_JOB_CREATED',
    GEARMAN_COMMAND_GRAB_JOB: 'GEARMAN_COMMAND_GRAB_JOB',

    # Gearman commands 10-19
    GEARMAN_COMMAND_NO_JOB: 'GEARMAN_COMMAND_NO_JOB',
    GEARMAN_COMMAND_JOB_ASSIGN: 'GEARMAN_COMMAND_JOB_ASSIGN',
    GEARMAN_COMMAND_WORK_STATUS: 'GEARMAN_COMMAND_WORK_STATUS',
    GEARMAN_COMMAND_WORK_COMPLETE: 'GEARMAN_COMMAND_WORK_COMPLETE',
    GEARMAN_COMMAND_WORK_FAIL: 'GEARMAN_COMMAND_WORK_FAIL',
    GEARMAN_COMMAND_GET_STATUS: 'GEARMAN_COMMAND_GET_STATUS',
    GEARMAN_COMMAND_ECHO_REQ: 'GEARMAN_COMMAND_ECHO_REQ',
    GEARMAN_COMMAND_ECHO_RES: 'GEARMAN_COMMAND_ECHO_RES',
    GEARMAN_COMMAND_SUBMIT_JOB_BG: 'GEARMAN_COMMAND_SUBMIT_JOB_BG',
    GEARMAN_COMMAND_ERROR: 'GEARMAN_COMMAND_ERROR',

    # Gearman commands 20-29
    GEARMAN_COMMAND_STATUS_RES: 'GEARMAN_COMMAND_STATUS_RES',
    GEARMAN_COMMAND_SUBMIT_JOB_HIGH: 'GEARMAN_COMMAND_SUBMIT_JOB_HIGH',
    GEARMAN_COMMAND_SET_CLIENT_ID: 'GEARMAN_COMMAND_SET_CLIENT_ID',
    GEARMAN_COMMAND_CAN_DO_TIMEOUT: 'GEARMAN_COMMAND_CAN_DO_TIMEOUT',
    GEARMAN_COMMAND_ALL_YOURS: 'GEARMAN_COMMAND_ALL_YOURS',
    GEARMAN_COMMAND_WORK_EXCEPTION: 'GEARMAN_COMMAND_WORK_EXCEPTION',
    GEARMAN_COMMAND_OPTION_REQ: 'GEARMAN_COMMAND_OPTION_REQ',
    GEARMAN_COMMAND_OPTION_RES: 'GEARMAN_COMMAND_OPTION_RES',
    GEARMAN_COMMAND_WORK_DATA: 'GEARMAN_COMMAND_WORK_DATA',
    GEARMAN_COMMAND_WORK_WARNING: 'GEARMAN_COMMAND_WORK_WARNING',

    # Gearman commands 30-39
    GEARMAN_COMMAND_GRAB_JOB_UNIQ: 'GEARMAN_COMMAND_GRAB_JOB_UNIQ',
    GEARMAN_COMMAND_JOB_ASSIGN_UNIQ: 'GEARMAN_COMMAND_JOB_ASSIGN_UNIQ',
    GEARMAN_COMMAND_SUBMIT_JOB_HIGH_BG: 'GEARMAN_COMMAND_SUBMIT_JOB_HIGH_BG',
    GEARMAN_COMMAND_SUBMIT_JOB_LOW: 'GEARMAN_COMMAND_SUBMIT_JOB_LOW',
    GEARMAN_COMMAND_SUBMIT_JOB_LOW_BG: 'GEARMAN_COMMAND_SUBMIT_JOB_LOW_BG',

    GEARMAN_COMMAND_TEXT_COMMAND: 'GEARMAN_COMMAND_TEXT_COMMAND'
}

GEARMAN_SERVER_COMMAND_STATUS = 'status'
GEARMAN_SERVER_COMMAND_VERSION = 'version'
GEARMAN_SERVER_COMMAND_WORKERS = 'workers'
GEARMAN_SERVER_COMMAND_MAXQUEUE = 'maxqueue'
GEARMAN_SERVER_COMMAND_SHUTDOWN = 'shutdown'

def get_command_name(cmd_type):
    return GEARMAN_COMMAND_TO_NAME.get(cmd_type, cmd_type)

def submit_cmd_for_background_priority(background, priority):
    cmd_type_lookup = {
        (True, PRIORITY_NONE): GEARMAN_COMMAND_SUBMIT_JOB_BG,
        (True, PRIORITY_LOW): GEARMAN_COMMAND_SUBMIT_JOB_LOW_BG,
        (True, PRIORITY_HIGH): GEARMAN_COMMAND_SUBMIT_JOB_HIGH_BG,
        (False, PRIORITY_NONE): GEARMAN_COMMAND_SUBMIT_JOB,
        (False, PRIORITY_LOW): GEARMAN_COMMAND_SUBMIT_JOB_LOW,
        (False, PRIORITY_HIGH): GEARMAN_COMMAND_SUBMIT_JOB_HIGH
    }
    lookup_tuple = (background, priority)
    cmd_type = cmd_type_lookup[lookup_tuple]
    return cmd_type

def parse_binary_command(in_buffer, is_response=True):
    """Parse data and return (command type, command arguments dict, command size)
    or (None, None, data) if there's not enough data for a complete command.
    """
    in_buffer_size = len(in_buffer)
    magic = None
    cmd_type = None
    cmd_args = None
    cmd_len = 0
    expected_packet_size = None

    # If we don't have enough data to parse, error early
    if in_buffer_size < COMMAND_HEADER_SIZE:
        return cmd_type, cmd_args, cmd_len

    # By default, we'll assume we're dealing with a gearman command
    magic, cmd_type, cmd_len = struct.unpack('!4sII', in_buffer[:COMMAND_HEADER_SIZE])

    received_bad_response = is_response and bool(magic != MAGIC_RES_STRING)
    received_bad_request = not is_response and bool(magic != MAGIC_REQ_STRING)
    if received_bad_response or received_bad_request:
        raise ProtocolError('Malformed Magic')

    expected_cmd_params = GEARMAN_PARAMS_FOR_COMMAND.get(cmd_type, None)

    # GEARMAN_COMMAND_TEXT_COMMAND is a faked command that we use to support server text-based commands
    if expected_cmd_params is None or cmd_type == GEARMAN_COMMAND_TEXT_COMMAND:
        raise ProtocolError('Received unknown binary command: %s' % cmd_type)

    # If everything indicates this is a valid command, we should check to see if we have enough stuff to read in our buffer
    expected_packet_size = COMMAND_HEADER_SIZE + cmd_len
    if in_buffer_size < expected_packet_size:
        return None, None, 0

    binary_payload = in_buffer[COMMAND_HEADER_SIZE:expected_packet_size]
    split_arguments = []

    if len(expected_cmd_params) > 0:
        split_arguments = binary_payload.split(NULL_CHAR, len(expected_cmd_params) - 1)
    elif binary_payload:
        raise ProtocolError('Expected no binary payload: %s' % get_command_name(cmd_type))

    # This is a sanity check on the binary_payload.split() phase
    # We should never be able to get here with any VALID gearman data
    if len(split_arguments) != len(expected_cmd_params):
        raise ProtocolError('Received %d argument(s), expecting %d argument(s): %s' % (len(split_arguments), len(expected_cmd_params), get_command_name(cmd_type)))

    # Iterate through the split arguments and assign them labels based on their order
    cmd_args = dict((param_label, param_value) for param_label, param_value in zip(expected_cmd_params, split_arguments))
    return cmd_type, cmd_args, expected_packet_size


def pack_binary_command(cmd_type, cmd_args, is_response=False):
    """Packs the given command using the parameter ordering specified in GEARMAN_PARAMS_FOR_COMMAND.
    *NOTE* Expects that all arguments in cmd_args are already str's.
    """
    expected_cmd_params = GEARMAN_PARAMS_FOR_COMMAND.get(cmd_type, None)
    if expected_cmd_params is None or cmd_type == GEARMAN_COMMAND_TEXT_COMMAND:
        raise ProtocolError('Received unknown binary command: %s' % get_command_name(cmd_type))

    expected_parameter_set = set(expected_cmd_params)
    received_parameter_set = set(cmd_args.keys())
    if expected_parameter_set != received_parameter_set:
        raise ProtocolError('Received arguments did not match expected arguments: %r != %r' % (expected_parameter_set, received_parameter_set))

    # Select the right expected magic
    if is_response:
        magic = MAGIC_RES_STRING
    else:
        magic = MAGIC_REQ_STRING

    # !NOTE! str should be replaced with bytes in Python 3.x
    # We will iterate in ORDER and str all our command arguments
    if compat.any(type(param_value) != str for param_value in cmd_args.itervalues()):
        raise ProtocolError('Received non-binary arguments: %r' % cmd_args)

    data_items = [cmd_args[param] for param in expected_cmd_params]
    binary_payload = NULL_CHAR.join(data_items)

    # Pack the header in the !4sII format then append the binary payload
    payload_size = len(binary_payload)
    packing_format = '!4sII%ds' % payload_size
    return struct.pack(packing_format, magic, cmd_type, payload_size, binary_payload)

def parse_text_command(in_buffer):
    """Parse a text command and return a single line at a time"""
    cmd_type = None
    cmd_args = None
    cmd_len = 0
    if '\n' not in in_buffer:
        return cmd_type, cmd_args, cmd_len

    text_command, in_buffer = in_buffer.split('\n', 1)
    if NULL_CHAR in text_command:
        raise ProtocolError('Received unexpected character: %s' % text_command)

    # Fake gearman command "TEXT_COMMAND" used to process server admin client responses
    cmd_type = GEARMAN_COMMAND_TEXT_COMMAND
    cmd_args = dict(raw_text=text_command)
    cmd_len = len(text_command) + 1

    return cmd_type, cmd_args, cmd_len

def pack_text_command(cmd_type, cmd_args):
    """Parse a text command and return a single line at a time"""
    if cmd_type != GEARMAN_COMMAND_TEXT_COMMAND:
        raise ProtocolError('Unknown cmd_type: Received %s, expecting %s' % (get_command_name(cmd_type), get_command_name(GEARMAN_COMMAND_TEXT_COMMAND)))

    cmd_line = cmd_args.get('raw_text')
    if cmd_line is None:
        raise ProtocolError('Did not receive arguments any valid arguments: %s' % cmd_args)

    return str(cmd_line)