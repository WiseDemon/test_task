from src.redis_protocol_error import RedisProtocolError
from src.exceptions.redis_data_parser_exceptions import *
from twisted.internet.defer import Deferred


class IDataParser:
    """
    Interface for data parsers
    """
    def parse(self, data: bytes) -> bytes:
        """
        Parse given data. When Parsing is complete,
        deferred is fired. Parser eats up data until
        it gets an error or parses a value
        :param data: data to parse
        :return remaining_data
        """
        pass

    def getDeferred(self) -> Deferred:
        """
        Return deferred which will fire when
        parsing is complete
        :return:
        """
        return self.defer


class RedisDataParser(IDataParser):
    """
    Class that parses data according to redis protocol
    """
    def __init__(self):
        self._cur_parser = None
        self.defer = Deferred()

    def fireDeferred(self, result):
        """
        Method to use as callback for
        internal parser deferred
        :param result:
        :return:
        """
        d = self.defer
        self.defer = Deferred()
        d.callback(result)
        self._cur_parser = None
        return result

    def parse(self, data:bytes) -> bytes:
        if data:
            if not self._cur_parser:
                if data[0] == ord('+'):
                    self._cur_parser = RedisDataStringParser()
                elif data[0] == ord('-'):
                    self._cur_parser = RedisDataErrorParser()
                elif data[0] == ord(':'):
                    self._cur_parser = RedisDataIntParser()
                elif data[0] == ord('$'):
                    self._cur_parser = RedisDataBulkStringParser()
                elif data[0] == ord('*'):
                    self._cur_parser = RedisDataArrayParser()
                else:
                    raise ParserFirstByteNotRecognized(f'First byte {data[0]} has no meaning')
                data = data[1:]
                d = self._cur_parser.getDeferred()
                d.addCallback(self.fireDeferred)

            data = self._cur_parser.parse(data)
        return data


class RedisDataStringParser(IDataParser):
    """
    Class for parsing simple string in Redis protocol.
    Simple strings start with '+'.
    """
    def __init__(self):
        self._s = ''
        self.defer = Deferred()

    def fireDeferred(self, result):
        d = self.defer
        self.defer = Deferred()
        d.callback(result)
        self._s = ''
        return result

    def parse(self, data: bytes) -> (str,bytes,bool):
        if data:
            try:
                decoded = data.decode('utf-8')
            except UnicodeDecodeError:
                return data
            else:
                crlf_pos = decoded.find('\r\n')
                if crlf_pos == -1:
                    if len(self._s) and self._s[-1] == '\r' and decoded[0] == '\n':
                        self._s = self._s[:-1]
                        data = data[1:]
                        self.fireDeferred(self._s)
                    else:
                        self._s += decoded
                        data = b''
                else:
                    self._s += decoded[0:crlf_pos]
                    data = decoded[crlf_pos + 2:].encode('utf-8')
                    self.fireDeferred(self._s)
        return data


class RedisDataErrorParser(RedisDataStringParser):
    """
    Class for parsing errors in Redis protocol.
    Errors are simple strings but start with '-'
    and end with '\r\n'
    """
    def fireDeferred(self, result):
        result = RedisProtocolError(result)
        d = self.defer
        self.defer = Deferred()
        d.callback(result)
        return result


class RedisDataIntParser(RedisDataStringParser):
    """
    Class for parsing int in Redis protocol.
    Ints start with ':' and end with '\r\n'
    """
    def fireDeferred(self, result):
        try:
            result = int(result)
        except ValueError:
            raise ParserValueError(f"can't convert '{result}' to int")
        d = self.defer
        self.defer = Deferred()
        d.callback(result)
        return result


class RedisDataBulkStringParser(IDataParser):
    """
    Class for parsing 'Bulk' strings in Redis protocol
    Bulk strings start with '${k}', followed by '\r\n', where k
    is bytelength of the string (not including '\r\n').
    After that comes the string itself, followed by '\r\n'
    """
    def __init__(self):
        self._bytelen = None
        self._bytelen_parser = RedisDataIntParser()
        self._bytelen_deferred = None
        self._setBytelen(None)
        self.defer = Deferred()

    def _setBytelen(self, bytelen):
        self._bytelen = bytelen
        self._bytelen_deferred = self._bytelen_parser.getDeferred()
        self._bytelen_deferred.addCallback(self._setBytelen)

    def fireDeferred(self, result):
        d = self.defer
        self.defer = Deferred()
        d.callback(result)
        self._bytelen = None
        return result

    def parse(self, data: bytes) -> (str,bytes,bool):
        if data:
            if self._bytelen is None:
                data = self._bytelen_parser.parse(data)

            if self._bytelen is not None:
                if self._bytelen == -1:
                    self.fireDeferred(None)
                elif len(data) >= self._bytelen + 2:
                    try:
                        value = data[:self._bytelen].decode('utf-8')
                    except UnicodeDecodeError:
                        raise ParserValueError(f'decoding of {data[:self._bytelen]} failed')
                    if data[self._bytelen: self._bytelen + 2] != b'\r\n':
                        raise ParserBulkStringWrongSize(f"expected b'\\r\\n' after {value}, found\
                            {data[self._bytelen: self._bytelen + 2]}")
                    data = data[self._bytelen + 2:]
                    self.fireDeferred(value)
        return data


class RedisDataArrayParser(IDataParser):
    """
    Class for parsing arrays in Redis protocol.
    Arrays start with '*{k}', followed by'\r\n'
    where k is the size of the array. After that
    come array elements, which are valid for separate
    parsing. For example, array can contain another array.
    """
    def __init__(self):
        self._size = None
        self._size_parser = RedisDataIntParser()
        self._size_deferred = None
        self._setSize(None)

        self._value = []
        self._array_parser = RedisDataParser()
        self._element_deferred = self._array_parser.getDeferred()
        self._element_deferred.addCallback(self._appendValue)

        self.defer = Deferred()

    def _setSize(self, size):
        """
        callback for size parser deferred
        :param size:
        :return:
        """
        self._size = size
        self._size_deferred = self._size_parser.getDeferred()
        self._size_deferred.addCallback(self._setSize)

    def _appendValue(self, value):
        """
        callvack for element parser deferred
        :param value:
        :return:
        """
        self._value.append(value)
        self._size -= 1
        self._element_deferred = self._array_parser.getDeferred()
        self._element_deferred.addCallback(self._appendValue)

    def fireDeferred(self, result):
        self.defer.callback(result)
        self.defer = Deferred()
        self._size = None
        self._value = []
        return result

    def parse(self, data: bytes) -> (list,bytes):
        if data:
            # First, we get the array size
            if self._size is None:
                data = self._size_parser.parse(data)

            # Then we parse data to extract self.size number of values
            if self._size is not None:
                if self._size == -1:
                    self.fireDeferred(None)
                elif self._size == 0:
                    self.fireDeferred([])
                elif self._size > 0:
                    cur_size = len(self._value)
                    data = self._array_parser.parse(data)
                    # parse while there is enough data to parse (new values appear)
                    # and array is not full
                    while self._size and cur_size != len(self._value):
                        cur_size = len(self._value)
                        data = self._array_parser.parse(data)

                    # When size is 0, then extraction of values is complete
                    if self._size == 0:
                        self.fireDeferred(self._value)
        return data
