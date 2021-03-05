import sys, getopt
from twisted.internet import reactor
from src.client_protocol import ClientProtocolFactory


help_msg =\
    '''
    Usage: client [-h] [--host h] [--port p]
        -h, -- help     see this message
        --host h        address of the server to connect to
        --port p        port to connect to
                        (default port is 6379)
    '''


if __name__ == '__main__':
    port = 6379
    host = '127.0.0.1'
    try:
        opts, args = getopt.getopt(sys.argv[1:],'h',['port=','host=', 'help'])
    except getopt.GetoptError as err:
        print('Usage: client [-h] [--host h] [--port p]')
        sys.exit(err.msg)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print(help_msg)
            sys.exit()
        if opt == '--port':
            port = arg
        if opt == '--host':
            host = arg
    reactor.connectTCP(host, port, ClientProtocolFactory())
    print(f"Connected to {host}:{port}")
    reactor.run()
