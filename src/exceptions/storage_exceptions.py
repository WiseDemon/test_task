class StorageException(Exception):
    """
    basic Storage class exception
    """
    pass


class StorageKeyError(StorageException):
    """
    Storage has no such key
    """
    def __init__(self, msg):
        if msg is None:
            msg = 'Key error'
        else:
            msg = 'Key error: ' + msg
        super().__init__(msg)


class StoragePatternError(StorageException):
    """
    Encountered error in pattern while matching
    """
    def __init__(self, msg):
        if msg is None:
            msg = 'Wrong pattern'
        else:
            msg = 'Wrong pattern: ' + msg
        super().__init__(msg)


class StorageFileError(StorageException):
    """
    When loading or saving keys from file goes wrong
    """
    def __init__(self, msg):
        if msg is None:
            msg = 'Storage file error'
        else:
            msg = 'Storage file error: ' + msg
        super().__init__(msg)
