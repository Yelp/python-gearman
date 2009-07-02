import re
import struct

DEFAULT_PORT = 4730

COMMANDS = {
     1: ("can_do", ["func"]),
    23: ("can_do_timeout", ["func", "timeout"]),
     2: ("cant_do", ["func"]),
     3: ("reset_abilities", []),
    22: ("set_client_id", ["client_id"]),
     4: ("pre_sleep", []),

     6: ("noop", []),
     7: ("submit_job", ["func", "uniq", "arg"]),
    21: ("submit_job_high", ["func", "uniq", "arg"]),
    18: ("submit_job_bg", ["func", "uniq", "arg"]),

     8: ("job_created", ["handle"]),
     9: ("grab_job", []),
    10: ("no_job", []),
    11: ("job_assign", ["handle", "func", "arg"]),

    12: ("work_status", ["handle", "numerator", "denominator"]),
    13: ("work_complete", ["handle", "result"]),
    14: ("work_fail", ["handle"]),

    15: ("get_status", ["handle"]),
    20: ("status_res", ["handle", "known", "running", "numerator", "denominator"]),

    16: ("echo_req", ["text"]),
    17: ("echo_res", ["text"]),

    19: ("error", ["err_code", "err_text"]),

    24: ("all_yours", []),
}
# Create a mapping of function name -> id, args
R_COMMANDS = dict((m[0], (mid, m[1])) for mid, m in COMMANDS.iteritems())

txt_command_re = re.compile("^[\w\n\r]+")

class ProtocolError(Exception):
    pass

def parse_command(data, response=True):
    """Parse data and return (function name, argument dict, command size)
    or (None, None, data) if there's not enough data for a complete command.
    """
    data_len = len(data)

    if data_len < 4:
        return None, None, 0

    if response:
        expected_magic = "\x00RES"
    else:
        expected_magic = "\x00REQ"

    magic = data[:4]
    if magic == expected_magic:
        if data_len >= 12:
            magic, typ, cmd_len = struct.unpack("!4sLL", data[0:12])
            if data_len < 12 + cmd_len:
                return None, None, 0
        else:
            return None, None, 0
    elif txt_command_re.match(data):
        if '\n' in data:
            cmd, data = data.split('\n', 1)
            return cmd.strip(), data, len(cmd)+1
        return None, None, 0
    else:
        raise ProtocolError("Malformed Magic")


    msg_spec = COMMANDS.get(typ, None)
    if not msg_spec:
        raise ProtocolError("Unknown message received: %d" % typ)

    nargs = len(msg_spec[1])
    data = (nargs > 0) and data[12:12 + cmd_len].split('\x00', nargs-1) or []
    if len(data) != nargs:
        raise ProtocolError("Received wrong number of arguments to %s" % msg_spec[0])

    kwargs = dict(
        ((msg_spec[1][i], data[i]) for i in range(nargs))
    )

    return msg_spec[0], kwargs, 12 + cmd_len

def pack_command(name, response=False, **kwargs):
    msg = R_COMMANDS[name]
    data = []
    for k in msg[1]:
        v = kwargs.get(k, "")
        if v is None:
            v = ""
        data.append(str(v))
    data = "\x00".join(data)
    if response:
        magic = "\x00RES"
    else:
        magic = "\x00REQ"
    return "%s%s" % (struct.pack("!4sII", magic, msg[0], len(data)), data)
