from twisted.trial import unittest
from src.redis_data_parser import RedisDataParser
from src.exceptions.redis_data_parser_exceptions import *
from twisted.internet import reactor
from src.redis_protocol_error import RedisProtocolError


class TestRedisDataParser(unittest.TestCase):
    """
    Class with test for RedisDataParser class
    """
    def test_wrong_first_byte(self):
        """
        Test failure when wrong first byte in data is given
        :return:
        """
        parser = RedisDataParser()
        data = b'!a\r\n'
        self.assertRaises(ParserFirstByteNotRecognized, parser.parse, data)
        #self.assertFailure(d, ParserFirstByteNotRecognized)
        #parser.parse(data)
        parser = RedisDataParser()
        # Wrong first byte in one element of the array
        data = b'*3\r\n+OK\r\n!a\r\n'
        self.assertRaises(ParserFirstByteNotRecognized, parser.parse, data)

    def _test_positive(self, data, expected):
        """
        Template function for simple positive tests
        :param data:
        :param expected:
        :return:
        """
        parser = RedisDataParser()
        d = parser.getDeferred()
        d.addCallback(self.assertEqual, expected)
        parser.parse(data)
        return d

    def test_simple_string(self):
        """
        Test correct parsing of a simple string
        :return:
        """
        data = b'+OK\r\n'
        return self._test_positive(data, 'OK')

    def test_simple_string_partial(self):
        """
        Test correct parsing of a simple string,
        when data is given partially
        :return:
        """
        parser = RedisDataParser()
        d = parser.getDeferred()
        d.addCallback(self.assertEqual, 'Hello, world!')
        data = b'+Hello, world!\r\n'
        data_buffer = b''
        for i in range(len(data)):
            data_buffer += data[i:i+1]
            data_buffer = parser.parse(data_buffer)
        return d

    def test_int(self):
        """
        Test correct parsing of an int
        :return:
        """
        data = b':123\r\n'
        return self._test_positive(data, 123)

    def test_int_partial(self):
        """
        Test correct parsing of an integer,
        when data is given partially
        :return:
        """
        parser = RedisDataParser()
        d = parser.getDeferred()
        d.addCallback(self.assertEqual, 123)
        data = b':123\r\n'
        data_buffer = b''
        for i in range(len(data)):
            data_buffer += data[i:i + 1]
            data_buffer = parser.parse(data_buffer)
        return d

    def test_int_failure(self):
        """
        Test failure when given not int after ':' first byte
        :return:
        """
        parser = RedisDataParser()
        data = b':abc\r\n'
        self.assertRaises(ParserValueError, parser.parse, data)

    def test_error_msg(self):
        """
        Test correct parsing of an error message
        :return:
        """
        parser = RedisDataParser()
        d = parser.getDeferred()
        data = b'-Some error\r\n'

        def compareError(error):
            self.assertEqual(RedisProtocolError, type(error))
            self.assertEqual('Some error', error.msg)
        d.addCallback(compareError)
        parser.parse(data)
        return d

    def test_bulk_string(self):
        """
        Test correct parsing of a bulk string
        :return:
        """
        defers = []
        data = b'$13\r\nHello, world!\r\n'
        defers.append(self._test_positive(data, 'Hello, world!'))

        parser = RedisDataParser()
        d = parser.getDeferred()
        defers.append(d)
        data = b'$13\r\nHello, world!\r\nabc'
        d.addCallback(self.assertEqual, 'Hello, world!')
        data = parser.parse(data)
        self.assertEqual(b'abc',data)
        return defers

    def test_bulk_string_partial(self):
        """
        Test correct parsing of a bulk string,
        when data is given partially
        :return:
        """
        parser = RedisDataParser()
        d = parser.getDeferred()
        d.addCallback(self.assertEqual, 'Hello, world!')
        data = b'$13\r\nHello, world!\r\n'
        data_buffer = b''
        for i in range(len(data)):
            data_buffer += data[i:i + 1]
            data_buffer = parser.parse(data_buffer)
        return d

    def test_bulk_string_failure(self):
        """
        Test failure when actual string size differs from
        given size
        :return:
        """
        parser = RedisDataParser()
        data = b'$13\r\nHello, world!abc\r\n'
        self.assertRaises(ParserBulkStringWrongSize, parser.parse, data)
        parser = RedisDataParser()
        data = b'$13\r\nHello, wor\r\n'
        self.assertEqual(b'Hello, wor\r\n', parser.parse(data))

    def test_bulk_string_none(self):
        """
        Test correct parsing of bulk string None value
        :return:
        """
        data = b'$-1\r\n'
        return self._test_positive(data, None)

    def test_array(self):
        """
        Test correct array parsing
        :return:
        """
        defers = []
        data = b'*0\r\n'
        defers.append(self._test_positive(data, []))
        # array with 3 elements
        data = b'*3\r\n+OK\r\n$2\r\nHi\r\n:42\r\n'
        defers.append(self._test_positive(data, ['OK', 'Hi', 42]))
        # array in array
        data = b'*2\r\n*2\r\n:1\r\n:2\r\n:3\r\n'
        defers.append(self._test_positive(data, [[1,2],3]))
        return defers

    def test_array_partial(self):
        """
        Test correct parsing of an array,
        when data is given partially
        :return:
        """
        parser = RedisDataParser()
        d = parser.getDeferred()
        d.addCallback(self.assertEqual, ['OK', 'Hi', 42])
        data = b'*3\r\n+OK\r\n$2\r\nHi\r\n:42\r\n'
        data_buffer = b''
        for i in range(len(data)):
            data_buffer += data[i:i + 1]
            data_buffer = parser.parse(data_buffer)
        return d


if __name__ == '__main__':
    import unittest as unit
    unit.main(verbosity=2)
