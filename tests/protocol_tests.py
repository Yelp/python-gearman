import array
import struct
import unittest

from gearman import protocol

from gearman.connection import GearmanConnection
from gearman.constants import JOB_PENDING, JOB_CREATED, JOB_FAILED, JOB_COMPLETE
from gearman.errors import ConnectionError, ServerUnavailable, ProtocolError

from tests._core_testing import _GearmanAbstractTest

class ProtocolBinaryCommandsTest(unittest.TestCase):
    #######################
    # Begin parsing tests #
    #######################
    def test_parsing_errors(self):
        malformed_command_buffer = "%sAAAABBBBCCCC"

        # Raise malformed magic exceptions
        self.assertRaises(
            ProtocolError,
            protocol.parse_binary_command,
            array.array("c", malformed_command_buffer % "DDDD")
        )
        self.assertRaises(
            ProtocolError,
            protocol.parse_binary_command,
            array.array("c", malformed_command_buffer % protocol.MAGIC_RES_STRING),
            is_response=False
        )
        self.assertRaises(
            ProtocolError,
            protocol.parse_binary_command,
            array.array("c", malformed_command_buffer % protocol.MAGIC_REQ_STRING),
            is_response=True
        )

        # Raise unknown command errors
        unassigned_gearman_command = 1234
        unknown_command_buffer = struct.pack('!4sII', protocol.MAGIC_RES_STRING, unassigned_gearman_command, 0)
        unknown_command_buffer = array.array("c", unknown_command_buffer)
        self.assertRaises(ProtocolError, protocol.parse_binary_command, unknown_command_buffer)

        # Raise an error on our imaginary GEARMAN_COMMAND_TEXT_COMMAND
        imaginary_command_buffer = struct.pack('!4sII4s', protocol.MAGIC_RES_STRING, protocol.GEARMAN_COMMAND_TEXT_COMMAND, 4, 'ABCD')
        imaginary_command_buffer = array.array("c", imaginary_command_buffer)
        self.assertRaises(ProtocolError, protocol.parse_binary_command, imaginary_command_buffer)

        # Raise an error on receiving an unexpected payload
        unexpected_payload_command_buffer = struct.pack('!4sII4s', protocol.MAGIC_RES_STRING, protocol.GEARMAN_COMMAND_NOOP, 4, 'ABCD')
        unexpected_payload_command_buffer = array.array("c", unexpected_payload_command_buffer)
        self.assertRaises(ProtocolError, protocol.parse_binary_command, unexpected_payload_command_buffer)

    def test_parsing_request(self):
        # Test parsing a request for a job (server side parsing)
        grab_job_command_buffer = struct.pack('!4sII', protocol.MAGIC_REQ_STRING, protocol.GEARMAN_COMMAND_GRAB_JOB_UNIQ, 0)
        grab_job_command_buffer = array.array("c", grab_job_command_buffer)
        cmd_type, cmd_args, cmd_len = protocol.parse_binary_command(grab_job_command_buffer, is_response=False)
        self.assertEquals(cmd_type, protocol.GEARMAN_COMMAND_GRAB_JOB_UNIQ)
        self.assertEquals(cmd_args, dict())
        self.assertEquals(cmd_len, len(grab_job_command_buffer))

    def test_parsing_without_enough_data(self):
        # Test that we return with nothing to do... received a partial packet
        not_enough_data_command_buffer = struct.pack('!4s', protocol.MAGIC_RES_STRING)
        not_enough_data_command_buffer = array.array("c", not_enough_data_command_buffer)
        cmd_type, cmd_args, cmd_len = protocol.parse_binary_command(not_enough_data_command_buffer)
        self.assertEquals(cmd_type, None)
        self.assertEquals(cmd_args, None)
        self.assertEquals(cmd_len, 0)

        # Test that we return with nothing to do... received a partial packet (expected binary payload of size 4, got 0)
        not_enough_data_command_buffer = struct.pack('!4sII', protocol.MAGIC_RES_STRING, protocol.GEARMAN_COMMAND_ECHO_RES, 4)
        not_enough_data_command_buffer = array.array("c", not_enough_data_command_buffer)
        cmd_type, cmd_args, cmd_len = protocol.parse_binary_command(not_enough_data_command_buffer)
        self.assertEquals(cmd_type, None)
        self.assertEquals(cmd_args, None)
        self.assertEquals(cmd_len, 0)

    def test_parsing_no_args(self):
        noop_command_buffer = struct.pack('!4sII', protocol.MAGIC_RES_STRING, protocol.GEARMAN_COMMAND_NOOP, 0)
        noop_command_buffer = array.array("c", noop_command_buffer)
        cmd_type, cmd_args, cmd_len = protocol.parse_binary_command(noop_command_buffer)
        self.assertEquals(cmd_type, protocol.GEARMAN_COMMAND_NOOP)
        self.assertEquals(cmd_args, dict())
        self.assertEquals(cmd_len, len(noop_command_buffer))

    def test_parsing_single_arg(self):
        echoed_string = 'abcd'
        echo_command_buffer = struct.pack('!4sII4s', protocol.MAGIC_RES_STRING, protocol.GEARMAN_COMMAND_ECHO_RES, 4, echoed_string)
        echo_command_buffer = array.array("c", echo_command_buffer)
        cmd_type, cmd_args, cmd_len = protocol.parse_binary_command(echo_command_buffer)
        self.assertEquals(cmd_type, protocol.GEARMAN_COMMAND_ECHO_RES)
        self.assertEquals(cmd_args, dict(data=echoed_string))
        self.assertEquals(cmd_len, len(echo_command_buffer))

    def test_parsing_single_arg_with_extra_data(self):
        echoed_string = 'abcd'
        excess_bytes = 5
        excess_data = echoed_string + (protocol.NULL_CHAR * excess_bytes)
        excess_echo_command_buffer = struct.pack('!4sII9s', protocol.MAGIC_RES_STRING, protocol.GEARMAN_COMMAND_ECHO_RES, 4, excess_data)
        excess_echo_command_buffer = array.array("c", excess_echo_command_buffer)

        cmd_type, cmd_args, cmd_len = protocol.parse_binary_command(excess_echo_command_buffer)
        self.assertEquals(cmd_type, protocol.GEARMAN_COMMAND_ECHO_RES)
        self.assertEquals(cmd_args, dict(data=echoed_string))
        self.assertEquals(cmd_len, len(excess_echo_command_buffer) - excess_bytes)

    def test_parsing_multiple_args(self):
        # Tests ordered argument processing and proper NULL_CHAR splitting
        expected_data = protocol.NULL_CHAR * 4
        binary_payload = protocol.NULL_CHAR.join(['test', 'function', 'identifier', expected_data])
        payload_size = len(binary_payload)

        uniq_command_buffer = struct.pack('!4sII%ds' % payload_size, protocol.MAGIC_RES_STRING, protocol.GEARMAN_COMMAND_JOB_ASSIGN_UNIQ, payload_size, binary_payload)
        uniq_command_buffer = array.array("c", uniq_command_buffer)
        cmd_type, cmd_args, cmd_len = protocol.parse_binary_command(uniq_command_buffer)
        self.assertEquals(cmd_type, protocol.GEARMAN_COMMAND_JOB_ASSIGN_UNIQ)
        self.assertEquals(cmd_args, dict(job_handle='test', task='function', unique='identifier', data=expected_data))
        self.assertEquals(cmd_len, len(uniq_command_buffer))

    #######################
    # Begin packing tests #
    #######################
    def test_packing_errors(self):
        # Assert we get an unknown command
        cmd_type = 1234
        cmd_args = dict()
        self.assertRaises(ProtocolError, protocol.pack_binary_command, cmd_type, cmd_args)

        # Assert we get a fake command
        cmd_type = protocol.GEARMAN_COMMAND_TEXT_COMMAND
        cmd_args = dict()
        self.assertRaises(ProtocolError, protocol.pack_binary_command, cmd_type, cmd_args)

        # Assert we get arg mismatch, got 1, expecting 0
        cmd_type = protocol.GEARMAN_COMMAND_GRAB_JOB
        cmd_args = dict(extra='arguments')
        self.assertRaises(ProtocolError, protocol.pack_binary_command, cmd_type, cmd_args)

        # Assert we get arg mismatch, got 0, expecting 1
        cmd_type = protocol.GEARMAN_COMMAND_JOB_CREATED
        cmd_args = dict()
        self.assertRaises(ProtocolError, protocol.pack_binary_command, cmd_type, cmd_args)

        # Assert we get arg mismatch (name), got 1, expecting 1
        cmd_type = protocol.GEARMAN_COMMAND_JOB_CREATED
        cmd_args = dict(extra='arguments')
        self.assertRaises(ProtocolError, protocol.pack_binary_command, cmd_type, cmd_args)

        # Assert we get a non-string argument
        cmd_type = protocol.GEARMAN_COMMAND_JOB_CREATED
        cmd_args = dict(job_handle=12345)
        self.assertRaises(ProtocolError, protocol.pack_binary_command, cmd_type, cmd_args)

        # Assert we get a non-string argument (expecting BYTES)
        cmd_type = protocol.GEARMAN_COMMAND_JOB_CREATED
        cmd_args = dict(job_handle=unicode(12345))
        self.assertRaises(ProtocolError, protocol.pack_binary_command, cmd_type, cmd_args)

        # Assert we check for NULLs in all but the "last" argument, where last depends on the cmd_type.
        cmd_type = protocol.GEARMAN_COMMAND_SUBMIT_JOB
        cmd_args = dict(task='funct\x00ion', data='abcd', unique='12345')
        self.assertRaises(ProtocolError, protocol.pack_binary_command, cmd_type, cmd_args)

        # Assert we check for NULLs in all but the "last" argument, where last depends on the cmd_type.
        cmd_type = protocol.GEARMAN_COMMAND_SUBMIT_JOB
        cmd_args = dict(task='function', data='ab\x00cd', unique='12345')
        protocol.pack_binary_command(cmd_type, cmd_args) # Should not raise, 'data' is last.

        # Assert we check for NULLs in all but the "last" argument, where last depends on the cmd_type.
        cmd_type = protocol.GEARMAN_COMMAND_SUBMIT_JOB
        cmd_args = dict(task='function', data='abcd', unique='123\x0045')
        self.assertRaises(ProtocolError, protocol.pack_binary_command, cmd_type, cmd_args)

    def test_packing_response(self):
        # Test packing a response for a job (server side packing)
        cmd_type = protocol.GEARMAN_COMMAND_NO_JOB
        cmd_args = dict()

        expected_command_buffer = struct.pack('!4sII', protocol.MAGIC_RES_STRING, cmd_type, 0)
        packed_command_buffer = protocol.pack_binary_command(cmd_type, cmd_args, is_response=True)
        self.assertEquals(packed_command_buffer, expected_command_buffer)

    def test_packing_no_arg(self):
        cmd_type = protocol.GEARMAN_COMMAND_NOOP
        cmd_args = dict()

        expected_command_buffer = struct.pack('!4sII', protocol.MAGIC_REQ_STRING, cmd_type, 0)
        packed_command_buffer = protocol.pack_binary_command(cmd_type, cmd_args)
        self.assertEquals(packed_command_buffer, expected_command_buffer)

    def test_packing_single_arg(self):
        cmd_type = protocol.GEARMAN_COMMAND_ECHO_REQ
        cmd_args = dict(data='abcde')

        expected_payload_size = len(cmd_args['data'])
        expected_format = '!4sII%ds' % expected_payload_size

        expected_command_buffer = struct.pack(expected_format, protocol.MAGIC_REQ_STRING, cmd_type, expected_payload_size, cmd_args['data'])
        packed_command_buffer = protocol.pack_binary_command(cmd_type, cmd_args)
        self.assertEquals(packed_command_buffer, expected_command_buffer)

    def test_packing_multiple_args(self):
        cmd_type = protocol.GEARMAN_COMMAND_SUBMIT_JOB
        cmd_args = dict(task='function', unique='12345', data='abcd')

        ordered_parameters = [cmd_args['task'], cmd_args['unique'], cmd_args['data']]

        expected_payload = protocol.NULL_CHAR.join(ordered_parameters)
        expected_payload_size = len(expected_payload)
        expected_format = '!4sII%ds' % expected_payload_size
        expected_command_buffer = struct.pack(expected_format, protocol.MAGIC_REQ_STRING, cmd_type, expected_payload_size, expected_payload)

        packed_command_buffer = protocol.pack_binary_command(cmd_type, cmd_args)
        self.assertEquals(packed_command_buffer, expected_command_buffer)

class ProtocolTextCommandsTest(unittest.TestCase):
	#######################
    # Begin parsing tests #
    #######################
    def test_parsing_errors(self):
        received_data = array.array("c", "Hello\x00there\n")
        self.assertRaises(ProtocolError, protocol.parse_text_command, received_data)

    def test_parsing_without_enough_data(self):
        received_data = array.array("c", "Hello there")
        cmd_type, cmd_response, cmd_len = protocol.parse_text_command(received_data)
        self.assertEquals(cmd_type, None)
        self.assertEquals(cmd_response, None)
        self.assertEquals(cmd_len, 0)

    def test_parsing_single_line(self):
        received_data = array.array("c", "Hello there\n")
        cmd_type, cmd_response, cmd_len = protocol.parse_text_command(received_data)
        self.assertEquals(cmd_type, protocol.GEARMAN_COMMAND_TEXT_COMMAND)
        self.assertEquals(cmd_response, dict(raw_text=received_data.tostring().strip()))
        self.assertEquals(cmd_len, len(received_data))

    def test_parsing_multi_line(self):
        sentence_one = array.array("c", "Hello there\n")
        sentence_two = array.array("c", "My name is bob\n")
        received_data = sentence_one + sentence_two

        cmd_type, cmd_response, cmd_len = protocol.parse_text_command(received_data)
        self.assertEquals(cmd_type, protocol.GEARMAN_COMMAND_TEXT_COMMAND)
        self.assertEquals(cmd_response, dict(raw_text=sentence_one.tostring().strip()))
        self.assertEquals(cmd_len, len(sentence_one))

    def test_packing_errors(self):
        # Test bad command type
        cmd_type = protocol.GEARMAN_COMMAND_NOOP
        cmd_args = dict()
        self.assertRaises(ProtocolError, protocol.pack_text_command, cmd_type, cmd_args)

        # Test missing args
        cmd_type = protocol.GEARMAN_COMMAND_TEXT_COMMAND
        cmd_args = dict()
        self.assertRaises(ProtocolError, protocol.pack_text_command, cmd_type, cmd_args)

        # Test misnamed parameter dict
        cmd_type = protocol.GEARMAN_COMMAND_TEXT_COMMAND
        cmd_args = dict(bad_text='abcdefghij')
        self.assertRaises(ProtocolError, protocol.pack_text_command, cmd_type, cmd_args)

    #######################
    # Begin packing tests #
    #######################
    def test_packing_single_line(self):
        expected_string = 'Hello world'
        cmd_type = protocol.GEARMAN_COMMAND_TEXT_COMMAND
        cmd_args = dict(raw_text=expected_string)

        packed_command = protocol.pack_text_command(cmd_type, cmd_args)
        self.assertEquals(packed_command, expected_string)

class GearmanConnectionTest(unittest.TestCase):
    """Tests the base CommandHandler class that underpins all other CommandHandlerTests"""
    def test_recv_command(self):
        pass

class GearmanCommandHandlerTest(_GearmanAbstractTest):
    """Tests the base CommandHandler class that underpins all other CommandHandlerTests"""
    def _test_recv_command(self):
        # recv_echo_res and recv_error are predefined on the CommandHandler
        self.command_handler.recv_command(protocol.GEARMAN_COMMAND_NOOP)
        self.assert_recv_command(protocol.GEARMAN_COMMAND_NOOP)

        # The mock handler never implemented 'recv_all_yours' so we should get an attribute error here
        self.assertRaises(ValueError, self.command_handler.recv_command, protocol.GEARMAN_COMMAND_ALL_YOURS)

    def _test_send_command(self):
        self.command_handler.send_command(protocol.GEARMAN_COMMAND_NOOP)
        self.assert_sent_command(protocol.GEARMAN_COMMAND_NOOP)

        # The mock handler never implemented 'recv_all_yours' so we should get an attribute error here
        self.command_handler.send_command(protocol.GEARMAN_COMMAND_ECHO_REQ, text='hello world')
        self.assert_sent_command(protocol.GEARMAN_COMMAND_ECHO_REQ, text='hello world')

    def assert_recv_command(self, expected_cmd_type, **expected_cmd_args):
        cmd_type, cmd_args = self.command_handler.recv_command_queue.popleft()
        self.assert_commands_equal(cmd_type, expected_cmd_type)
        self.assertEqual(cmd_args, expected_cmd_args)

    def assert_sent_command(self, expected_cmd_type, **expected_cmd_args):
        # All commands should be sent via the CommandHandler
        handler_cmd_type, handler_cmd_args = self.command_handler.sent_command_queue.popleft()
        self.assert_commands_equal(handler_cmd_type, expected_cmd_type)
        self.assertEqual(handler_cmd_args, expected_cmd_args)

        super(GearmanCommandHandlerTest, self).assert_sent_command(expected_cmd_type, **expected_cmd_args)


if __name__ == '__main__':
    unittest.main()
