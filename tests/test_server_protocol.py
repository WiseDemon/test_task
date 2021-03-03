from src.server_protocol import ServerProtocolFactory
from twisted.trial import unittest
from twisted.internet.testing import StringTransport, StringTransportWithDisconnection
from src.redis_encoder import RedisEncoder
from unittest.mock import patch


class TestServerProtocol(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = ServerProtocolFactory()
        self.proto = self.factory.buildProtocol(('127.0.0.1', 6379))
        self.tr = StringTransport()
        self.proto.makeConnection(self.tr)
        self.output = ''
        def fake_print(s):
            self.output = s
        self.fake_print = fake_print

    def test_connection_lost(self):
        factory = ServerProtocolFactory()
        proto = factory.buildProtocol(('127.0.0.1', 6379))
        tr = StringTransportWithDisconnection()
        tr.protocol = proto
        proto.makeConnection(self.tr)
        tr.loseConnection()
        self.assertEqual(0,factory.proto_count)

    def test_set_get(self):
        """
        Set and then get key
        :return:
        """
        data = RedisEncoder.encodeArray(['set', '1', 'one'])
        self.proto.dataReceived(data)
        self.assertEqual(b'+OK\r\n', self.tr.value())
        self.tr.clear()
        data = RedisEncoder.encodeArray(['get', '1'])
        self.proto.dataReceived(data)
        self.assertEqual(b'$3\r\none\r\n', self.tr.value())
        self.tr.clear()

    def test_get_None(self):
        """
        Get key that is not set
        :return:
        """
        data = RedisEncoder.encodeArray(['get', 'nonset'])
        self.proto.dataReceived(data)
        self.assertEqual(b'$-1\r\n', self.tr.value())
        self.tr.clear()

    def test_lrange_None(self):
        """
        Get array that is not set
        :return:
        """
        data = RedisEncoder.encodeArray(['lrange', 'nonset', '0', '1'])
        self.proto.dataReceived(data)
        self.assertEqual(b'*-1\r\n', self.tr.value())
        self.tr.clear()

    def test_wrong_first_byte(self):
        """
        Receive data with wrong first byte,
        then receive normal data.
        :return:
        """
        with patch('builtins.print', self.fake_print):
            data = b'abc'
            self.proto.dataReceived(data)
            self.assertEqual(b'', self.tr.value())
            self.assertEqual(b'', self.proto._data_buffer)
            data = RedisEncoder.encodeArray(['set', '1', 'one'])
            self.proto.dataReceived(data)
            self.assertEqual(b'+OK\r\n', self.tr.value())
            self.tr.clear()


if __name__ == '__main__':
    import unittest as unit
    unit.main(verbosity=2)
