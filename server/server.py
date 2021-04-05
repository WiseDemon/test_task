import sys, getopt
from signal import signal, SIGINT, SIGTERM

from twisted.internet import reactor

from src.server_protocol import ServerProtocolFactory
from src.exceptions.storage_exceptions import StorageFileError
from src.storage import Storage
from src.redis_command_parser import RedisCommandParser


help_msg =\
    '''
    Usage: server [-h] [--port p]
        -h, -- help     see this message
        --port p        set port p at which server listens
                        (default port is 6379)
        --save dest     set destination for saving storage keys
    '''

if __name__ == '__main__':
    print('Server starting...')
    port = 6379
    save_dest = './'

    # Reading options
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h', ['port=', 'save=', 'help'])
    except getopt.GetoptError as err:
        print('Usage: server [-h] [--port p] [--save dest]')
        sys.exit(err.msg)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print(help_msg)
            sys.exit()
        if opt == '--port':
            port = int(arg)
            print("Port is now", port)
        if opt == '--save':
            save_dest = arg + '/'
            print('Saving to', save_dest)

    # Creating storage
    try:
        storage = Storage(gc=True, file_prefix=save_dest+'storage')
    except StorageFileError as err:
        print(f"Error using save destination '{save_dest}': \n", str(err))
        print("Starting without disk saving/loading feature.")
        storage = Storage(gc=True)

    command_parser = RedisCommandParser(storage=storage)
    factory = ServerProtocolFactory(parser=command_parser)

    listening_port = reactor.listenTCP(port, factory)

    # CTRL+C handling
    def sigint_handler(signal_recieved, frame):
        listening_port.stopListening()
        try:
            factory.parser.storage.save()
        except StorageFileError as err:
            print(err)
            print('Keys are not saved')
        reactor.stop()
        print('Bye')

    signal(SIGINT, sigint_handler)
    signal(SIGTERM, sigint_handler)
    print('Server is up')
    reactor.run()
