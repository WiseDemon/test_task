import unittest
from src.redis_encoder import RedisEncoder


class TestEncoding(unittest.TestCase):
    """
    Test RedisEncoder class
    """
    def test_simple_string(self):
        self.assertEqual(b'+OK\r\n', RedisEncoder.encodeString('OK'))

    def test_bulk_string(self):
        self.assertEqual(b'$2\r\nHi\r\n', RedisEncoder.encodeBulkString('Hi'))

    def test_bulk_string_none(self):
        self.assertEqual(b'$-1\r\n',RedisEncoder.encodeBulkString(None))

    def test_int(self):
        self.assertEqual(b':123\r\n', RedisEncoder.encodeInt(123))

    def test_array(self):
        self.assertEqual(b'*2\r\n$2\r\nHi\r\n$2\r\nHo\r\n', RedisEncoder.encodeArray(['Hi', 'Ho']))
        self.assertEqual(b'*3\r\n$2\r\nHi\r\n:1\r\n*2\r\n:1\r\n:2\r\n', RedisEncoder.encodeArray(['Hi', 1, [1,2]]))

    def test_array_none(self):
        self.assertEqual(b'*-1\r\n', RedisEncoder.encodeArray(None))


if __name__ == '__main__':
    unittest.main(verbosity=2)