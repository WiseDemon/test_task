class RedisProtocolError:
    """
    Simple error with message
    """
    def __init__(self, msg: str = None):
        self.msg = msg

    def __str__(self):
        return self.msg
