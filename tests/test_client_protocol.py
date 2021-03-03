from src.client_protocol import ClientProtocolFactory
from twisted.trial import unittest
from twisted.internet.testing import StringTransport
from twisted.internet.address import IPv4Address
from src.redis_encoder import RedisEncoder
from unittest.mock import patch


class TestServerProtocol(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = ClientProtocolFactory()
        self.addr = IPv4Address('TCP', '127.0.0.1', 50000)
        self.tr = StringTransport()

        self.input = ''
        self.output = ''
        def fake_input(s):
            return self.input
        def fake_print(s):
            self.output = s
        self.fake_input = fake_input
        self.fake_print = fake_print

    def test_send_receive(self):
        """
        Test sending and receiving data
        """
        with patch('builtins.input', self.fake_input):
            with patch('builtins.print', self.fake_print):
                self.proto = self.factory.buildProtocol(self.addr)

                self.input = 'set 1 one'
                self.proto.makeConnection(self.tr)
                command = RedisEncoder.encodeArray(['set','1','one'])
                self.assertEqual(command, self.tr.value())
                self.tr.clear()

                self.input = 'get 1'
                self.proto.dataReceived(b'+OK\r\n')
                self.assertEqual('OK', self.output)
                command = RedisEncoder.encodeArray(['get', '1'])
                self.assertEqual(command, self.tr.value())

    def test_exit(self):
        """
        Test disconnecting on 'exit' prompt
        """
        with patch('builtins.input', self.fake_input):
            with patch('builtins.print', self.fake_print):
                self.proto = self.factory.buildProtocol(self.addr)

                self.input = 'exit'
                self.proto.makeConnection(self.tr)
                self.assertEqual(True, self.tr.disconnecting)

    def test_disconnect_on_garbage(self):
        """
        Test disconnecting when garbage received (no first bit in data)
        """
        with patch('builtins.input', self.fake_input):
            with patch('builtins.print', self.fake_print):
                self.proto = self.factory.buildProtocol(self.addr)

                self.input = 'set 1 one'
                self.proto.makeConnection(self.tr)
                command = RedisEncoder.encodeArray(['set','1','one'])
                self.assertEqual(command, self.tr.value())
                self.tr.clear()

                self.proto.dataReceived(b'OK\r\n')
                self.assertEqual(True, self.tr.disconnecting)


if __name__ == '__main__':
    import unittest as unit
    unit.main(verbosity=2)
