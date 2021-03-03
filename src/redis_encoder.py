from src.exceptions.redis_encoder_exceptions import *


class RedisEncoder:
    """
    Class with static methods to encode various types
    according to redis protocol
    """
    @staticmethod
    def encode(val) -> bytes:
        """
        Encode object of type str, int, list or Exception.
        Strings are encoded as BulkStrings.
        Better use specific methods.
        """
        if isinstance(val, str):
            data = RedisEncoder.encodeBulkString(val)
        elif isinstance(val, int):
            data = RedisEncoder.encodeInt(val)
        elif isinstance(val, list):
            data = RedisEncoder.encodeArray(val)
        elif isinstance(val, Exception):
            data = RedisEncoder.encodeError(val)
        else:
            raise RedisEncoderWrongType(f"can't encode type {type(val)}.")
        return data

    @staticmethod
    def encodeError(err:Exception) -> bytes:
        """
        Method for encoding errors to send.
        Errors start with b'-' and end with b'\r\n'
        :param err: Exception to encode
        :return: encoded data as bytes
        """
        if not isinstance(err, Exception):
            raise RedisEncoderWrongType(f"encodeError takes Exception as argument, got {type(err)}")
        data = b'-' + str(err).encode('utf-8') + b'\r\n'
        return data

    @staticmethod
    def encodeString(s: str) -> bytes:
        """
        Encode simple string.
        Simple strings start with b'+'
        and end with b'\r\n'
        :param s: str to encode
        :return: encoded data as bytes
        """
        if not isinstance(s, str):
            raise RedisEncoderWrongType(f"encodeString takes str as argument, got {type(s)}")
        data = b'+' + s.encode('utf-8') + b'\r\n'
        return data

    @staticmethod
    def encodeInt(n: int) -> bytes:
        """
        Encode integer.
        Integers start with b':' and end with b'\r\n'
        :param n:
        :return: encoded data as bytes
        """
        if not isinstance(n, int):
            raise RedisEncoderWrongType(f"encodeInt takes int as argument, got {type(n)}")
        data = b':' + str(n).encode('utf-8') + b'\r\n'
        return data

    @staticmethod
    def encodeBulkString(s: str) -> bytes:
        """
        Method for encoding bulk strings.
        Bulk strings start with b'${k}\r\n',
        where k is bytelength of the string.
        Then follows string itself, ending with b'\r\n'
        :param s: str to encode
        :return: encoded data as bytes
        """
        if s is None:
            return b'$-1\r\n'
        if not isinstance(s, str):
            raise RedisEncoderWrongType(f"encodeBulkString takes str as argument, got {type(s)}")
        data = s.encode('utf-8')
        data = b'$' + str(len(data)).encode('utf-8') + b'\r\n' + data + b'\r\n'
        return data

    @staticmethod
    def encodeArray(arr: list) -> bytes:
        """
        Encode array.
        Arrays start with b'*{k}\r\n',
        where k is number of elements in the array
        :param arr: array to encode
        :return: encoded data as bytes
        """
        if arr is None:
            return b'*-1\r\n'
        if not isinstance(arr, list):
            raise RedisEncoderWrongType(f"encodeArray takes list as argument, got {type(arr)}")
        parts = []
        for item in arr:
            if type(item) is str:
                parts.append(RedisEncoder.encodeBulkString(item))
            elif type(item) is int:
                parts.append(RedisEncoder.encodeInt(item))
            elif type(item) is list:
                parts.append(RedisEncoder.encodeArray(item))
            else:
                raise RedisEncoderWrongType(f"can't encode type {type(item)} in array")
        data = b'*' + str(len(arr)).encode('utf-8') + b'\r\n' + b''.join(parts)
        return data
