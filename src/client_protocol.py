from src.redis_protocol import RedisProtocol
from twisted.internet.protocol import ClientFactory
from twisted.internet import reactor
from src.exceptions.redis_data_parser_exceptions import RedisDataParserException
import shlex


class ClientProtocol(RedisProtocol):
    def connectionMade(self):
        self.inputAndSend()

    def inputAndSend(self):
        """
        Wait for user input, then encode it and send
        :return:
        """
        s = input(f'{self.addr.host}:{self.addr.port}>')
        if s.lower() == 'exit':
            self.factory.exit = True
            self.transport.loseConnection()
        data = self.encodeCommand(s)
        self.sendData(data)

    def _valueParsed(self, value):
        super()._valueParsed(value)
        print(value)
        self.inputAndSend()

    def _parseBuffer(self):
        """
        Overload method to close connection when
        wrong data is recieved
        :return:
        """
        try:
            self._data_buffer = self._parser.parse(self._data_buffer)
        except RedisDataParserException as err:
            print(err)
            self.transport.loseConnection()

    def encodeCommand(self, command:str) -> bytes:
        words = shlex.split(command, posix=True)
        s = f'*{len(words)}\r\n'
        data = s.encode('utf-8')
        for word in words:
            byteword = word.encode('utf-8')
            data += b'$' + str(len(byteword)).encode('utf-8') + b'\r\n' +\
                byteword + b'\r\n'
        return data


class ClientProtocolFactory(ClientFactory):
    protocol = ClientProtocol

    def buildProtocol(self, addr):
        prot = self.protocol()
        prot.addr = addr
        prot.factory = self
        self.exit = False
        return prot

    def clientConnectionFailed(self, connector, reason):
        print(reason)
        reactor.stop()

    def clientConnectionLost(self, connector, reason):
        if self.exit:
            print('Bye')
        else:
            print(reason)
        reactor.stop()
