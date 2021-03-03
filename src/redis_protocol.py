from twisted.internet.protocol import Protocol
from src.redis_data_parser import RedisDataParser
from src.exceptions.redis_data_parser_exceptions import RedisDataParserException


class RedisProtocol(Protocol):
    """
    Class that implements Redis protocol
    """
    def __init__(self):
        self._data_buffer = b''
        self._parser = RedisDataParser()
        self._parser_defer = self._parser.getDeferred()
        self._parser_defer.addCallback(self._valueParsed)

    def dataReceived(self, data):
        self._data_buffer += data
        self._parseBuffer()

    def sendData(self, data: bytes):
        self.transport.write(data)

    def _parseBuffer(self):
        """
        Parse data buffer with RedisDataParser class.
        When some value is completely parsed, _valueParsed is called
        with the value as argument.
        :return:
        """
        try:
            self._data_buffer = self._parser.parse(self._data_buffer)
        except RedisDataParserException as err:
            print(err)
            self._data_buffer = b''
            self._parser = RedisDataParser()
            self._parser_defer = self._parser.getDeferred()
            self._parser_defer.addCallback(self._valueParsed)

    def _valueParsed(self, value):
        """
        callback for parsing a value. When inheriting,
        call it from super or get new deferred yourself
        :return:
        """
        self._parser_defer = self._parser.getDeferred()
        self._parser_defer.addCallback(self._valueParsed)
