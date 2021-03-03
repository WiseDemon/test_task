class RedisCommandParserException(Exception):
    """
    Basic RedisCommandParser class exception
    """
    pass


class WrongCommand(RedisCommandParserException):
    """
    Exception for wrong command given
    """
    def __init__(self, msg=None):
        if msg is None:
            msg = 'Wrong command'
        else:
            msg = 'Wrong command: ' + msg
        super().__init__(msg)


class CommandSyntaxError(RedisCommandParserException):
    """
    Wrong syntax for given command
    """
    def __init__(self, msg=None):
        if msg is None:
            msg = 'Syntax error'
        else:
            msg = 'Syntax error: ' + msg
        super().__init__(msg)


class CommandWrongType(RedisCommandParserException):
    """
    Wrong type of argument for given command
    """
    def __init__(self, msg=None):
        if msg is None:
            msg = 'Wrong type'
        else:
            msg = 'Wrong type: ' + msg
        super().__init__(msg)


class CommandWrongArgumentNumber(RedisCommandParserException):
    """
    Wrong number of arguments for given command
    """
    def __init__(self, msg=None):
        if msg is None:
            msg = 'Wrong arguments'
        else:
            msg = 'Wrong arguments: ' + msg
        super().__init__(msg)


class CommandKeyError(RedisCommandParserException):
    """
    No such key
    """
    def __init__(self, msg=None):
        if msg is None:
            msg = 'Key error'
        else:
            msg = 'Key error: ' + msg
        super().__init__(msg)


class CommandOutOfRange(RedisCommandParserException):
    """
    Hit out of range when performing operation on a list
    """
    def __init__(self, msg=None):
        if msg is None:
            msg = 'Out of range'
        else:
            msg = 'Out of range: ' + msg
        super().__init__(msg)
