import unittest
from unittest.mock import patch
import time
from src.exceptions.redis_command_parser_exceptions import *
from src.redis_command_parser import RedisCommandParser, CommandParserSuccess, ArrayNone, BulkStringNone


class TestCommandParser(unittest.TestCase):
    """
    Class for testing CommandParser class
    """
    def setUp(self) -> None:
        self.now = time.time()

        def fake_time():
            return self.now
        self.fake_time = fake_time

    def test_wrong_command(self):
        """
        Test if the 'WrongCommand' exception is raised when
        calling 'parse' with args starting with wrong command
        :return:
        """
        parser = RedisCommandParser()
        self.assertRaises(WrongCommand, parser.parse, 'dsfsdfs 1 2 3'.split(' '))
        self.assertRaises(WrongCommand, parser.parse, ['eeee'])

    def test_set(self):
        """
        Test 'set' command for positive outcome (without ttl)
        :return:
        """
        parser = RedisCommandParser()
        self.assertEqual(CommandParserSuccess, parser.parse('set 1 one'.split(' ')))
        self.assertEqual('one', parser.parse('set 1 one GET'.split(' ')))
        self.assertEqual(BulkStringNone, parser.parse('set 2 two GET'.split(' ')))
        self.assertEqual(CommandParserSuccess, parser.parse('set 2 "two" EX 5'.split(' ')))
        self.assertEqual(BulkStringNone, parser.parse('set 1 one NX'.split(' ')))
        self.assertEqual(CommandParserSuccess, parser.parse('set 1 one XX'.split(' ')))
        self.assertEqual(CommandParserSuccess, parser.parse('set 1 one KEEPTTL'.split(' ')))
        self.assertEqual(CommandParserSuccess, parser.parse('SET 1 one keepttl'.split(' ')))
        self.assertEqual(CommandParserSuccess, parser.parse('Set 1 one Keepttl'.split(' ')))

    def test_set_wrong_arguments(self):
        """
        Test for 'set' failure due to wrong arguments
        in multiple cases
        :return:
        """
        parser = RedisCommandParser()
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['set'])
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, 'set key'.split(' '))

    def test_set_wrong_syntax(self):
        """
        Test for 'set' failure due to wrong syntax
        usage: SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL]
            [NX|XX] [GET]
        :return:
        """
        parser = RedisCommandParser()
        self.assertRaises(CommandSyntaxError, parser.parse, 'set key value1 value2'.split(' '))
        self.assertRaises(CommandSyntaxError, parser.parse, 'set key value value EX'.split(' '))
        self.assertRaises(CommandSyntaxError, parser.parse, 'set key value EX'.split(' '))
        self.assertRaises(CommandSyntaxError, parser.parse, 'set key value EX 5 PX 5'.split(' '))
        self.assertRaises(CommandSyntaxError, parser.parse, 'set key value EX 5 EXAT 100'.split(' '))
        self.assertRaises(CommandSyntaxError, parser.parse, 'set key value NX XX'.split(' '))
        self.assertRaises(CommandSyntaxError, parser.parse, 'set key value EX 5 KEEPTTL'.split(' '))
        self.assertRaises(CommandSyntaxError, parser.parse, 'set key value NX 5'.split(' '))

    def test_get_failure(self):
        """
        Test for 'get' failure
        :return:
        """
        parser = RedisCommandParser()
        # wrong arguments
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['get'])
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, 'get key1 key2'.split(' '))
        # wrong type
        parser.storage.set('1', [1,2,3])
        self.assertRaises(CommandWrongType, parser.parse, ['get', '1'])

    def test_get(self):
        """
        Test 'get' command for positive outcome (without ttl)
        :return:
        """
        parser = RedisCommandParser()
        self.assertEqual(BulkStringNone, parser.parse('get 1'.split(' ')))
        parser.storage.set('1', 'one')
        self.assertEqual('one', parser.parse('get 1'.split(' ')))

    def test_set_get(self):
        """
        Test 'set' and 'get' commands together (without ttl)
        :return:
        """
        parser = RedisCommandParser()
        self.assertEqual(BulkStringNone, parser.parse('get 1'.split(' ')))
        self.assertEqual(CommandParserSuccess, parser.parse('set 1 one'.split(' ')))
        self.assertEqual('one', parser.parse('get 1'.split(' ')))
        self.assertEqual(BulkStringNone, parser.parse('set 1 two NX'.split(' ')))
        self.assertEqual('one', parser.parse('get 1'.split(' ')))
        self.assertEqual(BulkStringNone,parser.parse('set 2 two XX'.split(' ')))
        self.assertEqual(BulkStringNone, parser.parse('get 2'.split(' ')))
        self.assertEqual(CommandParserSuccess, parser.parse('set 1 two XX'.split(' ')))
        self.assertEqual('two', parser.parse('get 1'.split(' ')))

    def test_set_get_ttl(self):
        """
        Test 'set' and 'get' commands with ttl
        :return:
        """
        self.now = time.time()
        with patch('time.time', self.fake_time):
            parser = RedisCommandParser()
            parser.parse('set 1 one ex 5'.split(' '))
            parser.parse('set 2 two ex 10'.split(' '))
            parser.parse('set 3 three'.split(' '))
            self.assertEqual('one', parser.parse('get 1'.split(' ')))
            self.assertEqual('two', parser.parse('get 2'.split(' ')))
            self.assertEqual('three', parser.parse('get 3'.split(' ')))
            self.now += 6
            self.assertEqual(BulkStringNone, parser.parse('get 1'.split(' ')))
            self.assertEqual('two', parser.parse('get 2'.split(' ')))
            self.assertEqual('three', parser.parse('get 3'.split(' ')))
            self.now += 5
            self.assertEqual(BulkStringNone, parser.parse('get 1'.split(' ')))
            self.assertEqual(BulkStringNone, parser.parse('get 2'.split(' ')))
            self.assertEqual('three', parser.parse('get 3'.split(' ')))

    def test_keys(self):
        """
        Test 'keys' command for positive outcome (without ttl)
        :return:
        """
        parser = RedisCommandParser()
        self.assertEqual([], parser.parse('keys *'.split(' ')))
        parser.storage.set('abc', 'one')
        parser.storage.set('abcd', 'two')
        parser.storage.set('abd', 'three')
        self.assertEqual([], parser.parse('keys ?'.split(' ')))
        self.assertEqual({'abc', 'abcd', 'abd'}, set(parser.parse(['keys', '*'])))
        self.assertEqual({'abc','abd'}, set(parser.parse(['keys', 'ab?'])))
        self.assertEqual({'abcd'}, set(parser.parse(['keys', 'abc?'])))
        self.assertEqual({'abc','abcd','abd'}, set(parser.parse(['keys', 'ab*'])))
        self.assertEqual({'abc', 'abcd', 'abd'}, set(parser.parse(['keys', 'ab*'])))
        self.assertEqual({'abc', 'abd'}, set(parser.parse(['keys', 'ab[cd]'])))
        self.assertEqual({'abc'}, set(parser.parse(['keys', 'ab[^d]'])))
        self.assertEqual({'abc', 'abcd'}, set(parser.parse(['keys', 'ab[^d-e]*'])))
        self.assertEqual({'abc', 'abd'}, set(parser.parse(['keys', 'ab[a-e]'])))
        self.assertEqual(set(), set(parser.parse(['keys', 'ab[^a-e]'])))

    def test_keys_failure(self):
        """
        Test 'keys' command failure
        :return:
        """
        parser = RedisCommandParser()
        # wrong arguments
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['keys'])
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['keys','abc', 'aaa'])
        # wrong pattern
        parser.storage.set('abc',1)
        self.assertRaises(CommandSyntaxError, parser.parse, ['keys','a[b'])

    def test_del(self):
        """
        Test 'del' command for positive outcome (without ttl)
        :return:
        """
        parser = RedisCommandParser()
        self.assertEqual(0, parser.parse('del 1'.split(' ')))
        parser.storage.set('1','one')
        self.assertEqual(1, parser.parse('del 1'.split(' ')))
        self.assertEqual(0, parser.parse('del 1'.split(' ')))
        parser.storage.set('1', 'one')
        parser.storage.set('2', 'two')
        self.assertEqual(2, parser.parse('del 1 2 3'.split(' ')))

    def test_del_wrong_arguments(self):
        """
        Test for 'del' command failure due to wrong arguments
        :return:
        """
        parser = RedisCommandParser()
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['del'])

    def test_lrange(self):
        """
        Test 'lrange' command positive outcome
        :return:
        """
        parser = RedisCommandParser()
        parser.storage.set('1', [1,2,3])
        self.assertEqual([1,2,3], parser.parse('lrange 1 0 -1'.split(' ')))
        self.assertEqual([1, 2], parser.parse('lrange 1 0 1'.split(' ')))
        self.assertEqual([2, 3], parser.parse('lrange 1 -2 -1'.split(' ')))
        self.assertEqual([1], parser.parse('lrange 1 0 0'.split(' ')))

    def test_lrange_failure(self):
        """
        Test 'lrange' failure
        :return:
        """
        parser = RedisCommandParser()
        # wrong arguments
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['lrange', 'key', '1'])
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['lrange', 'key', '1', '2', '3'])
        # wrong key type
        parser.storage.set('1', 'hi')
        self.assertRaises(CommandWrongType, parser.parse, ['lrange', '1', '0', '1'])
        # start or stop are not int
        parser.storage.set('1', [1,2,3])
        self.assertRaises(CommandSyntaxError, parser.parse, ['lrange', '1', 'abc', '1'])
        self.assertRaises(CommandSyntaxError, parser.parse, ['lrange', '1', '0', 'abc'])

    def test_lpush(self):
        """
        Test 'lpush' positive outcome
        :return:
        """
        parser = RedisCommandParser()
        self.assertEqual(3, parser.parse('lpush list1 1 2 3'.split(' ')))
        self.assertEqual(5, parser.parse('lpush list1 4 5'.split(' ')))
        self.assertEqual(['5','4','3','2','1'], parser.storage.get('list1'))

    def test_lpush_failure(self):
        """
        Test 'lpush' failure
        :return:
        """
        parser = RedisCommandParser()
        # wrong arguments
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['lpush', 'list1'])
        # wrong key type
        parser.storage.set('list1', 1)
        self.assertRaises(CommandWrongType, parser.parse, ['lpush', 'list1', '1'])

    def test_rpush(self):
        """
        Test 'rpush' positive outcome
        :return:
        """
        parser = RedisCommandParser()
        self.assertEqual(3, parser.parse('rpush list1 1 2 3'.split(' ')))
        self.assertEqual(5, parser.parse('rpush list1 4 5'.split(' ')))
        self.assertEqual(['1','2','3','4','5'], parser.storage.get('list1'))

    def test_rpush_failure(self):
        """
        Test 'rpush' failure
        :return:
        """
        parser = RedisCommandParser()
        # wrong arguments
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['rpush', 'list1'])
        # wrong key type
        parser.storage.set('list1', 1)
        self.assertRaises(CommandWrongType, parser.parse, ['rpush', 'list1', '1'])

    def test_lset(self):
        """
        Test 'lset' positive outcome
        :return:
        """
        parser = RedisCommandParser()
        parser.storage.set('list1', ['1','2','3'])
        self.assertEqual(CommandParserSuccess, parser.parse(['lset', 'list1', '1', '42']))
        self.assertEqual(['1','42','3'], parser.storage.get('list1'))

    def test_lset_failure(self):
        """
        Test 'lset; failure
        :return:
        """
        parser = RedisCommandParser()
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['lset', 'list1'])
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['lset', 'list1', '1', '2', '3'])
        # wrong key type
        parser.storage.set('list1', '1')
        self.assertRaises(CommandWrongType, parser.parse, ['lset', 'list1','1', '42'])
        # key error
        self.assertRaises(CommandKeyError, parser.parse, ['lset', 'list2', '1', '42'])
        # index out of range
        parser.storage.set('list2', ['1','2','3'])
        self.assertRaises(CommandOutOfRange, parser.parse, ['lset', 'list2', '3', '42'])
        # index is not int
        self.assertRaises(CommandSyntaxError, parser.parse, ['lset', 'list2', 'abc', '42'])

    def test_lget(self):
        """
        Test 'lget' positive outcome
        :return:
        """
        parser = RedisCommandParser()
        parser.storage.set('list1', ['1','2','3'])
        self.assertEqual('2', parser.parse(['lget', 'list1', '1']))

    def test_lget_failure(self):
        """
        Test 'lget; failure
        :return:
        """
        parser = RedisCommandParser()
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['lget', 'list1'])
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['lget', 'list1', '1', '2'])
        # wrong key type
        parser.storage.set('list1', '1')
        self.assertRaises(CommandWrongType, parser.parse, ['lget', 'list1','1'])
        # key error
        self.assertRaises(CommandKeyError, parser.parse, ['lget', 'list2', '1'])
        # index out of range
        parser.storage.set('list2', ['1','2','3'])
        self.assertRaises(CommandOutOfRange, parser.parse, ['lget', 'list2', '3'])
        # index is not int
        self.assertRaises(CommandSyntaxError, parser.parse, ['lget', 'list2', 'abc'])

    def test_hset(self):
        """
        Test 'hset' positive outcome
        :return:
        """
        parser = RedisCommandParser()
        self.assertEqual(2, parser.parse('hset dict1 1 one 2 two'.split(' ')))
        self.assertEqual(1, parser.parse('hset dict1 1 one 3 three'.split(' ')))

    def test_hset_failure(self):
        """
        Test 'hset' failure
        :return:
        """
        parser = RedisCommandParser()
        # wrong arguments
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, 'hset dict1'.split(' '))
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, 'hset dict1 f1 1 f2'.split(' '))
        # wrong key type
        parser.storage.set('dict1', '1')
        self.assertRaises(CommandWrongType, parser.parse, 'hset dict1 1 one'.split(' '))

    def test_hget(self):
        """
        Test 'hget' positive outcome
        :return:
        """
        parser = RedisCommandParser()
        parser.storage.set('dict1', {'1':'one', '2':'two'})
        self.assertEqual('one', parser.parse('hget dict1 1'.split(' ')))
        self.assertEqual('two', parser.parse('hget dict1 2'.split(' ')))
        self.assertEqual(BulkStringNone, parser.parse('hget dict1 3'.split(' ')))

    def test_hget_failure(self):
        """
        Test 'hget' failure
        :return:
        """
        parser = RedisCommandParser()
        # wrong arguments
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, 'hget dict1'.split(' '))
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, 'hget dict1 1 2'.split(' '))
        # wrong key type
        parser.storage.set('dict1', '1')
        self.assertRaises(CommandWrongType, parser.parse, 'hget dict1 1'.split(' '))

    def test_expire(self):
        """
        Test 'expire' positive outcome
        :return:
        """
        self.now = time.time()
        with patch('time.time', self.fake_time):
            parser = RedisCommandParser()
            parser.storage.set('1', 'one')
            self.assertEqual(1, parser.parse(['expire', '1', '5']))
            self.assertEqual(0, parser.parse('expire 2 5'.split(' ')))
            self.now += 6
            self.assertEqual(BulkStringNone, parser.parse(['get', '1']))

    def test_expire_failure(self):
        """
        Test 'expire' failure
        :return:
        """
        parser = RedisCommandParser()
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['expire', '1'])
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['expire', '1', '2', '3'])

    def test_persist(self):
        """
        Test 'persist' positive outcome
        :return:
        """
        self.now = time.time()
        with patch('time.time',self.fake_time):
            parser = RedisCommandParser()
            parser.storage.set('1', 'one', self.now + 5)
            parser.storage.set('2', 'two')
            self.assertEqual(1, parser.parse(['persist', '1']))
            self.assertEqual(0, parser.parse(['persist', '2']))
            self.assertEqual(0, parser.parse(['persist', '3']))
            self.now += 6
            self.assertEqual('one', parser.parse(['get', '1']))

    def test_persist_failure(self):
        """
        Test 'persist' failure
        :return:
        """
        parser = RedisCommandParser()
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['persist'])
        self.assertRaises(CommandWrongArgumentNumber, parser.parse, ['persist', '1', '2'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
