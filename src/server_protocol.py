from src.redis_protocol import RedisProtocol
from twisted.internet.protocol import ServerFactory
from src.redis_command_parser import *
from src.redis_encoder import RedisEncoder
from src.exceptions.redis_command_parser_exceptions import *
from src.exceptions.server_protocol_exceptions import *


class ServerProtocol(RedisProtocol):
    def __init__(self, factory):
        super().__init__()
        self.factory = factory

    def connectionMade(self):
        self.factory.proto_count += 1

    def connectionLost(self, reason):
        self.factory.proto_count -= 1

    def _valueParsed(self, value):
        super()._valueParsed(value)
        try:
            result = self.factory.parser.parse(value)
        except RedisCommandParserException as err:
            result = err
        data = self._encodeResult(result)
        self.sendData(data)

    def _encodeResult(self, result):
        """
        Method for encoding result as bytes according
        to Redis protocol
        :return:
        """
        if result is BulkStringNone:
            ans = RedisEncoder.encodeBulkString(None)
        elif result is ArrayNone:
            ans = RedisEncoder.encodeArray(None)
        elif result is CommandParserSuccess:
            ans = RedisEncoder.encodeString('OK')
        elif isinstance(result, Exception):
            ans = RedisEncoder.encodeError(result)
        elif isinstance(result, list):
            ans = RedisEncoder.encodeArray(result)
        elif isinstance(result, int):
            ans = RedisEncoder.encodeInt(result)
        elif isinstance(result, str):
            ans = RedisEncoder.encodeBulkString(result)
        else:
            raise UnidentifiedParserResult(f"Don't know to encode result of type {type(result)}.")
        return ans


class ServerProtocolFactory(ServerFactory):
    protocol = ServerProtocol

    def __init__(self, parser=None):
        """
        :param parser: RedisCommandParser object or None, to create it automatically
        :param gc: enable garbage collector in the created parser
        :param file_prefix: prefix for file names for storage saving/loading,
            set None to disable saving/loading
        """
        self.proto_count = 0
        if parser is None:
            parser = RedisCommandParser()
        self.parser = parser

    def buildProtocol(self, addr):
        return self.protocol(self)
