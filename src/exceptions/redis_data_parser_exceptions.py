class RedisDataParserException(Exception):
    """
    Basic exception for data parsers
    """
    pass


class ParserValueError(RedisDataParserException):
    """
    When parser fails to convert parsed value
    from string to another type
    """

    def __init__(self, msg=None):
        if msg is None:
            msg = 'Parser value error'
        else:
            msg = 'Parser value error: ' + msg
        super().__init__(msg)


class ParserFirstByteNotRecognized(RedisDataParserException):
    """
    There is no ongoing parsing and first byte of the
    data does not corresponds to any type of parsing
    """

    def __init__(self, msg=None):
        if msg is None:
            msg = 'First byte not recognized'
        else:
            msg = 'First byte not recognized: ' + msg
        super().__init__(msg)


class ParserBulkStringWrongSize(RedisDataParserException):
    """
    Actual size of the string differs from given ('\r\n' is not
    found right after the string)
    """
    def __init__(self, msg=None):
        if msg is None:
            msg = 'First byte not recognized'
        else:
            msg = 'First byte not recognized: ' + msg
        super().__init__(msg)

