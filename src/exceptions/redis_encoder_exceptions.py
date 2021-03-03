class RedisEncoderException(Exception):
    """
    Basic RedisEncoder class exception
    """
    pass


class RedisEncoderWrongType(Exception):
    """
    Wrong type for the encoder method
    """
    def __init__(self, msg):
        if msg is None:
            msg = 'Encoder wrong type'
        else:
            msg = 'Encoder wrong type: ' + msg
        super().__init__(msg)
