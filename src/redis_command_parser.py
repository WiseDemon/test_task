from src.storage import Storage
from src.exceptions.redis_command_parser_exceptions import *
from src.exceptions.storage_exceptions import *
import time

# Object to return in case of success
CommandParserSuccess = object()

# Different None objects for string and list,
# to encode them accordingly
BulkStringNone = object()
ArrayNone = object()


class RedisCommandParser:
    """
    Class for parsing Redis commands
    """
    def __init__(self, storage=None, gc=False, file_prefix=None):
        """
        :param storage: Storage object or None for creating it automatically
        :param gc: enable garbage collector in createt Storage
        :param file_prefix: prefix for file names for storage saving/loading,
            set None to disable saving/loading
        """
        if storage is None:
            try:
                storage = Storage(gc=gc,file_prefix=file_prefix)
            except StorageFileError as err:
                print(err)
                print("Key saving disabled")
                storage = Storage(gc=gc,file_prefix=None)
        self.storage = storage
        self.astonished = False

    def parse(self, args: list):
        """
        Parses string command and returns a result of it's execution.
        Available commands: GET, SET, DEL, KEY, LRANGE, LPUSH, RPUSH, LSET, LGET, HSET, HGET, EXPIRE, PERSIST
        :return: result of the specified command
        :exception RedisCommandParserException: specific exceptions are in _parse_ methods
        :exception WrongCommand: when the specified command isn't found
        """
        # just one time print when there is A LOT of arguments
        if not self.astonished and len(args) > 100:
            print('dude wtf')
            self.astonished = True

        command = args[0].lower()
        try:
            op = getattr(self,'_parse_' + command)
        except AttributeError:
            raise WrongCommand(f"unknown command `{command}`")
        else:
            return op(args[1:])

    def _parse_set(self, args):
        """
        Parse arguments for SET command.
        Set key to hold the string value. If key already holds a value, it is overwritten,
        regardless of its type. Any previous time to live associated with the key is discarded
        on successful SET operation.

        Usage: SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL]
            [NX|XX] [GET]
        Options:
            EX seconds -- Set the specified expire time, in seconds.
            PX milliseconds -- Set the specified expire time, in milliseconds.
            EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
            PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds.
            NX -- Only set the key if it does not already exist.
            XX -- Only set the key if it already exist.
            KEEPTTL -- Retain the time to live associated with the key.
            GET -- Return the old value stored at key, or nil when key did not exist.
        :param args:
        :return: CommandParserSuccess object if SET was executed correctly. Previous value of the key if option GET
        is set. None if there is no previous value for GET or if NX/XX are used but conditions are
        not met (in case of NX GET still returns value).
        :exception CommandWrongArgumentNumber: when number of arguments is less than 2
        :exception CommandSyntaxError: when there is syntax error in the command
        """
        if len(args) < 2:
            raise CommandWrongArgumentNumber(f'`set` command needs 2 arguments, found {len(args)}')
        key = args[0]
        value = args[1]
        opts = {'existence': 0,
                'moe': None,
                'keep_moe': False,
                'get': False}
        pos = 2
        # parsing remaining arguments
        while pos < len(args):
            opt = args[pos].lower()
            # ttl options
            if opt in ('ex','px','exat','pxat'):
                if opts['moe']:
                    raise CommandSyntaxError('only one of EX, PX, EXAT, PXAT options may be present')
                if pos + 1 >= len(args):
                    raise CommandSyntaxError(f'{opt.upper()} option takes 1 int argument, none found')
                try:
                    opts['moe'] = int(args[pos+1])
                except ValueError:
                    raise CommandSyntaxError(f'{opt.upper()} option takes 1 int argument, `{args[pos+1]}`\
                     found instead')
                if opt == 'ex':
                    opts['moe'] += time.time()
                elif opt == 'px':
                    opts['moe'] = opts['moe']*1e-3 + time.time()
                elif opt == 'pxat':
                    opts['moe'] *= 1e-3
                pos += 1
            # key existence options: 1 for 'set if not exists',
            # 2 for 'set if exists', 0 for neither
            elif opt in ('xx', 'nx'):
                if opts['existence']:
                    raise CommandSyntaxError(f'only one of XX, NX options may be present')
                if opt == 'nx':
                    opts['existence'] = 1
                else:
                    opts['existence'] = 2
            elif opt == 'keepttl':
                if opts['moe']:
                    raise CommandSyntaxError(f'KEEPTTL option may not be present with \
                    EX, PX, EXAT, PXAT options')
                opts['keep_moe'] = True
            elif opt == 'get':
                opts['get'] = True
            else:
                raise CommandSyntaxError(f'{opt} option not found')
            pos += 1

        ans = CommandParserSuccess
        do_set = True
        if opts['existence'] or opts['get']:
            try:
                prev_val = self.storage.get(key)
            except StorageKeyError:
                prev_val = None
                if opts['existence'] == 2:
                    do_set = False
                    ans = None
            else:
                if opts['existence'] == 1:
                    do_set = False
                    ans = None
            if opts['get']:
                ans = prev_val
        if do_set:
            self.storage.set(key, value, moe=opts['moe'], keep_moe=opts['keep_moe'])
        if ans is None:
            ans = BulkStringNone
        return ans

    def _parse_get(self, args):
        """
        Parse arguments for GET command.
        Return value of a key holding string value
        Usage: GET key
        :param args:
        :return: value of the key or BulkStringNone if there is no such key
        or it's expired
        :exception CommandWrongArgumentNumber: when there is not exactly 1 argument
        :exception CommandWrongType: trying to get a key storing something other
            than string
        """
        if len(args) != 1:
            raise CommandWrongArgumentNumber(f'`get` command needs 1 argument, found {len(args)}')
        try:
            ans = self.storage.get(args[0])
        except StorageKeyError:
            ans = None
        if ans is None:
            ans = BulkStringNone
        elif type(ans) is not str:
            raise CommandWrongType(f'`get` command only operates with keys holding string values')
        return ans

    def _parse_keys(self, args):
        """
        Parse arguments for KEYS command.
        Returns a list of keys from storage matching pattern.
        Usage: KEYS pattern
        :param args:
        :return: list of keys or empty list
        :exception CommandWrongArgumentNumber: not exactly 1 argument given
        :exception CommandSyntaxError: encountered error in pattern while matching
        """
        if len(args) != 1:
            raise CommandWrongArgumentNumber(f'`keys` command needs 1 argument, found {len(args)}')
        try:
            ans = self.storage.keys(args[0])
        except StoragePatternError:
            raise CommandSyntaxError('error in pattern')
        else:
            return ans

    def _parse_del(self, args):
        """
        Parse arguments for DEL command.
        Deletes a set of keys.
        Usage: DEL key1 [key2 ...]
        :param args:
        :return: The number of deleted keys
        :exception CommandWrongArgumentNumber: less than 1 argument was given
        """
        if not len(args):
            raise CommandWrongArgumentNumber('`del` command needs at least 1 argument')
        return self.storage.delete(args)

    def _parse_lrange(self, args):
        """
        Parse arguments for LRANGE command.
        Returns the specified elements of the list stored under the key.
        Usage: LRANGE key start stop
        :param args:
        :return: list of values in range of the specified indexes, ArrayNone
        if there is no such key
        :exception CommandWrongArgumentNumber: not exactly 3 arguments given
        :exception CommandWrongType: specified key holds non-list value
        :exception CommandSyntaxError: start and stop not integers
        """
        if len(args) != 3:
            raise CommandWrongArgumentNumber(f'`lrange` command needs 3 argument, found {len(args)}')
        try:
            ans = self.storage.get(args[0])
        except StorageKeyError:
            ans = None
        if ans is None:
            ans = ArrayNone
        elif type(ans) is not list:
            raise CommandWrongType(f'`lrange` command only operates with keys holding list values')
        else:
            try:
                start = int(args[1])
                stop = int(args[2])
            except ValueError:
                raise CommandSyntaxError('start and stop must be integers')
            if stop == -1:
                ans = ans[start:]
            else:
                ans = ans[start:stop+1]
        return ans

    def _parse_lpush(self,args):
        """
        Parse arguments for LPUSH command
        Adds specified elements to existing list from left
        or creates new one if there is none
        Usage: LPUSH key val1 [val2 ...]
        :param args:
        :return: Length of the list after insertion
        :exception CommandWrongArgumentNumber: less than 2 argument given
        :exception CommandWrongType: specified key holds non-list value
        """
        if len(args) < 2:
            raise CommandWrongArgumentNumber(f'`lpush` command needs at lest 2 argument, found {len(args)}')
        try:
            val = self.storage.get(args[0])
        except StorageKeyError:
            val = args[-1:0:-1]
            self.storage.set(args[0], val)
            return len(val)
        else:
            if type(val) is not list:
                raise CommandWrongType(f'`lpush` command only operates with keys holding list values')
            else:
                val = args[-1:0:-1] + val
                self.storage.set(args[0], val, keep_moe=True)
                return len(val)

    def _parse_rpush(self,args):
        """
        Parse arguments for RPUSH command
        Adds specified elements to existing list from right
        or creates new one if there is none
        Usage: RPUSH key val1 [val2 ...]
        :param args:
        :return: Length of the list after insertion
        :exception CommandWrongArgumentNumber: less than 2 argument given
        :exception CommandWrongType: specified key holds non-list value
        """
        if len(args) < 2:
            raise CommandWrongArgumentNumber(f'`rpush` command needs at lest 2 argument, found {len(args)}')
        try:
            val = self.storage.get(args[0])
        except StorageKeyError:
            val = args[1:]
            self.storage.set(args[0], val)
            return len(val)
        else:
            if type(val) is not list:
                raise CommandWrongType(f'`rpush` command only operates with keys holding list values')
            else:
                val.extend(args[1:])
                return len(val)

    def _parse_lset(self, args):
        """
        Sets specified element in a list
        Usage: LSET key index value
        :param args:
        :return: CommandParserSuccess
        :exception CommandWrongArgumentNumber: not exactly 3 arguments given
        :exception CommandWrongType: specified key holds non-list value
        :exception CommandKeyError: no such key
        :exception CommandOutOfRange: given index is out of range
        :exception CommandSyntaxError: index is not int
        """
        if len(args) != 3:
            raise CommandWrongArgumentNumber(f'`lset` command needs 3 argument, found {len(args)}')
        try:
            index = int(args[1])
        except ValueError:
            raise CommandSyntaxError('index must be integer')
        val = args[2]
        try:
            lval = self.storage.get(args[0])
        except StorageKeyError:
            raise CommandKeyError('no such key')
        else:
            if type(lval) is not list:
                raise CommandWrongType(f'`lset` command only operates with keys holding list values')
            elif index >= len(lval):
                raise CommandOutOfRange(f'index {index} is out of range, array of size {len(lval)}')
            else:
                lval[index] = val
                return CommandParserSuccess

    def _parse_lget(self, args):
        """
        Get value at index in list.
        Usage: LGET key index
        :param args:
        :return: the value at the specified index
        :exception CommandWrongArgumentNumber: no exactly 2 arguments given
        :exception CommandKeyError: no such key
        :exception CommandOutOfRange: given index is out of range
        :exception CommandWrongType: specified key holds non-list value
        :exception CommandSyntaxError: index is not int
        """
        if len(args) != 2:
            raise CommandWrongArgumentNumber(f'`lget` command needs 2 argument, found {len(args)}')
        try:
            index = int(args[1])
        except ValueError:
            raise CommandSyntaxError('index must be integer')

        try:
            lval = self.storage.get(args[0])
        except StorageKeyError:
            raise CommandKeyError(f'no such key')
        if type(lval) is not list:
            raise CommandWrongType(f'`lget` command only operates with keys holding list values')
        elif index >= len(lval):
            raise CommandOutOfRange(f'index {index} is out of range of array size {len(lval)}')
        else:
            return lval[index]

    def _parse_hset(self,args):
        """
        Sets field in the hash stored at key to value.
        If key does not exist, a new key holding a hash is created.
        If field already exists in the hash, it is overwritten.
        Usage: HSET key field1 val1 [field2 val2 ...]
        :param args:
        :return: the number of fields that were added
        :exception CommandWrongArgumentNumber: the number of arguments is even (one field has no value)
            or when there is less than 3 arguments
        :exception CommandWrongType: specified key holds non-dict value
        """
        if len(args) < 3:
            raise CommandWrongArgumentNumber(f'`hset` command needs at least 3 arguments, found {len(args)}')
        elif len(args) % 2 != 1:
            raise CommandWrongArgumentNumber(f'`hset` command needs odd number of arguments (key + pairs field-value),\
                found {len(args)}')
        try:
            hval = self.storage.get(args[0])
        except StorageKeyError:
            hval = dict((args[i], args[i+1]) for i in range(1,len(args)-1, 2))
            self.storage.set(args[0], hval)
            return len(hval)

        if type(hval) is not dict:
            raise CommandWrongType(f'`hget` command only operates with keys holding dict values')
        new_vals = dict((args[i], args[i+1]) for i in range(1,len(args)-1, 2))
        count = 0
        for key in new_vals:
            if key not in hval:
                count += 1
        hval.update(new_vals)
        return count

    def _parse_hget(self, args):
        """
        Get value from hash in a specified field
        Usage: HGET key field
        :param args:
        :return: value in the field
        :exception CommandWrongArgumentNumber: not exactly 2 arguments given
        :exception CommandWrongType: specified key holds non-dict value
        """
        if len(args) != 2:
            raise CommandWrongArgumentNumber(f'`hget` command needs 2 argument, found {len(args)}')
        try:
            hval = self.storage.get(args[0])
        except StorageKeyError:
            val = None
        else:
            if type(hval) is not dict:
                raise CommandWrongType(f'`hget` command only operates with keys holding dict values')
            else:
                try:
                    val = hval[args[1]]
                except KeyError:
                    val = None
        if val is None:
            return BulkStringNone
        else:
            return val

    def _parse_expire(self, args):
        """
        Set a timeout on key. After the timeout has expired,
        the key will automatically be deleted.
        Usage: EXPIRE key seconds
        :param args:
        :return: 1 if the timeout was set
                 0 if key does not exist
        :exception CommandWrongArgumentNumber: not exactly 2 arguments given
        """
        if len(args) != 2:
            raise CommandWrongArgumentNumber(f'`expire` command needs 2 arguments, found {len(args)}')
        moe = time.time() + int(args[1])
        try:
            self.storage.set_moe(args[0], moe)
        except StorageKeyError:
            return 0
        else:
            return 1

    def _parse_persist(self, args):
        """
        Remove the existing timeout on key
        Usage: PERSIST key
        :param args:
        :return: 1 if the timeout was removed
                 0 if key does not exist or does not have a timeout
        :exception CommandWrongArgumentNumber: not exactly 1 argument given
        """
        if len(args) != 1:
            raise CommandWrongArgumentNumber(f'`persist` command needs 1 arguments, found {len(args)}')
        try:
            _, moe = self.storage.get_val_and_moe(args[0])
        except StorageKeyError:
            return 0
        else:
            if moe is None:
                return 0
            else:
                self.storage.set_moe(args[0], None)
                return 1
