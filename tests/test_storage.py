import unittest
import time
from src.storage import Storage, StorageGarbageCollector
from unittest.mock import patch
from src.exceptions.storage_exceptions import *


class TestStorage(unittest.TestCase):
    """
    Class for testing Storage class
    """
    def setUp(self) -> None:
        self.now = time.time()

        def fake_time():
            return self.now

        self.fake_time = fake_time

    def test_set(self):
        """
        Test Storage.set basic functionality.
        :return:
        """
        storage = Storage()
        items_to_add = {'test_s': 'hello',
                        'test_list': [1,2,'three'],
                        'test_dict': {1:'one', 2:'two'}}
        moes = {'test_list': time.time() + 5}
        for key in items_to_add.keys():
            storage.set(key, items_to_add[key], moes.get(key))

        self.assertEqual(items_to_add, storage._keys_dict, 'Added keys are wrong.')
        self.assertEqual(moes, storage._moe_dict, 'Added moes are wrong.')

    def test_set_moe_rewrite(self):
        """
        Test if moe is deleted when rewriting key with moe
        to key without moe.
        :return:
        """
        storage = Storage()
        storage.set('1', 1, 5)
        storage.set('1', 2)
        self.assertEqual(False, '1' in storage._moe_dict, "Moe for key '1' should be reset.")

    def test_set_with_get(self):
        """
        Test Storage.set with 'get' option
        :return:
        """
        storage = Storage()
        storage.set('1', 1)
        self.assertEqual(1, storage.set('1', 2, get=True), "Should return previous value")
        self.assertEqual(2, storage.get('1'), 'Should get new value')
        self.assertEqual(None, storage.set('2', 1, get=True), "Should return None as there was no key '2'")

    def test_get(self):
        """
        Test Storage.get basic functionality (without ttl).
        :return:
        """
        storage = Storage()
        keys_to_set = {'1': 'hello',
                       '2': 'bye',
                       '3': [1,2,'three'],
                       '4': {1:'one', 2:'two'}}
        for key in keys_to_set.keys():
            storage.set(key, keys_to_set[key])

        values = [storage.get(key) for key in keys_to_set.keys()]
        true_values = [keys_to_set[key] for key in keys_to_set.keys()]
        self.assertEqual(true_values, values)
        self.assertRaises(StorageKeyError,storage.get, '0')

    def test_get_ttl(self):
        """
        Test Storage.get with ttl.
        :return:
        """
        self.now = time.time()
        with patch('time.time', self.fake_time):
            storage = Storage()
            keys_to_set = {'1': 'hello',
                           '2': 'bye',
                           '3': [1, 2, 'three'],
                           '4': {1: 'one', 2: 'two'}}
            moes = {'1': time.time() + 5, '4': time.time() + 10}
            for key in keys_to_set.keys():
                storage.set(key, keys_to_set[key], moes.get(key))
            # test at moment t
            self.assertEqual(keys_to_set['1'], storage.get('1'), "Key '1' should still exist.")
            # test at moment t+6, one key should expire
            self.now += 6
            keys_to_set.pop('1')
            moes.pop('1')
            self.assertRaises(StorageKeyError, storage.get, '1')
            self.assertEqual(keys_to_set['4'], storage.get('4'), "Key '4' should still exist.")
            self.assertEqual(keys_to_set, storage._keys_dict, "Remaining keys are wrong")
            self.assertEqual(moes, storage._moe_dict, "Remaining moes are wrong")
            # test at moment t+11
            self.now += 5
            keys_to_set.pop('4')
            moes.pop('4')
            self.assertRaises(StorageKeyError, storage.get, '1')
            self.assertRaises(StorageKeyError, storage.get, '4')
            self.assertEqual(keys_to_set, storage._keys_dict, "Remaining keys are wrong")
            self.assertEqual(moes, storage._moe_dict, "Remaining moes are wrong")

    def test_delete(self):
        '''
        Test Storage.delete basic functionality (without ttl).
        :return:
        '''
        storage = Storage()
        self.assertEqual(0, storage.delete([]))
        keys_to_set = {'1': 'hello',
                       '2': 'bye',
                       '3': [1, 2, 'three'],
                       '4': {1: 'one', 2: 'two'}}
        keys_to_delete = ['2', '4', '60']
        for key in keys_to_set:
            storage.set(key, keys_to_set[key])
        num_deleted = storage.delete(keys_to_delete)

        true_num_deleted = 0
        for key in keys_to_delete:
            if key in keys_to_set:
                keys_to_set.pop(key)
                true_num_deleted += 1
        self.assertEqual(true_num_deleted, num_deleted, 'Num of deleted keys is wrong.')
        self.assertEqual(keys_to_set, storage._keys_dict, 'Remaining keys are wrong')

    def test_keys(self):
        """
        Test Storage.keys basic functionality (without ttl)
        with redis-style pattern matching.
        :return:
        """
        storage = Storage()
        keys_to_set = {'1': 'hello',
                       '2': 'bye',
                       '3': [1, 2, 'three'],
                       '4': {1: 'one', 2: 'two'},
                       'a': 1,
                       'aa': 2,
                       'abc':3,
                       'hello':4}
        for key in keys_to_set:
            storage.set(key, keys_to_set[key])

        pattern_answers = {'?': ['1','2','3','4','a'],
                           '*': list(keys_to_set.keys()),
                           '[13]': ['1', '3'],
                           '[^a]': ['1','2','3','4'],
                           '[1-3]': ['1','2','3'],
                           '?[ae]*': ['aa', 'hello']}
        for pattern in pattern_answers:
            self.assertEqual(pattern_answers[pattern],
                             storage.keys(pattern), f'For pattern "{pattern}" expected {pattern_answers[pattern]}.')

    def test_keys_failure(self):
        """
        Test Storage.keys command for failure
        :return:
        """
        storage = Storage()
        storage._keys_dict = {'1': 'one',
                              'abc': '1'}
        self.assertRaises(StoragePatternError, storage.keys, 'ab[cd')

    def test_keys_ttl(self):
        """
        Test Storage.keys with ttl
        :return:
        """
        self.now = time.time()
        with patch('time.time', self.fake_time):
            storage = Storage()
            storage.set('1', 'one', self.now + 5)
            storage.set('2', 'two')
            storage.set('3', 'three', self.now + 10)
            self.now += 6
            self.assertEqual(['2','3'], storage.keys('*'))
            self.assertEqual(['2','3'], list(storage._keys_dict.keys()))

    def test_set_moe(self):
        """
        Test Storage.set_moe method
        :return:
        """
        self.now = time.time()
        with patch('time.time', self.fake_time):
            storage = Storage()
            self.assertRaises(StorageKeyError, storage.set_moe, 1, {'moe': 2})
            storage.set(1, 'one')
            storage.set(2, 'two', 2)
            self.assertEqual(None, storage.set_moe(1, 2))
            self.assertEqual(None, storage.set_moe(2, None))
            self.now += 3
            self.assertRaises(StorageKeyError, storage.get, 1)
            self.assertEqual('two', storage.get(2))


class TestGarbageCollector(unittest.TestCase):
    def setUp(self) -> None:
        self.storage = Storage()
        self.gc = StorageGarbageCollector(self.storage)
        self.now = time.time()

        def fake_time():
            return self.now

        self.fake_time = fake_time

    def test_one_key(self):
        """
        Test expiration works properly
        for storage with one key
        :return:
        """
        self.now = time.time()
        with patch('time.time', self.fake_time):
            self.storage.set('1', 'one', moe=self.now + 1)
            self.now += 2
            self.gc.expire_random()
            self.assertRaises(StorageKeyError, self.storage.get, '1')

    def test_many_expired_keys(self):
        """
        Test expiration of many keys simultaneously
        :return:
        """
        self.now = time.time()
        with patch('time.time', self.fake_time):
            for i in range(20):
                self.storage.set(i, i, moe=self.now + 1)
            self.now += 2
            self.gc.expire_random()
            for i in range(20):
                self.assertRaises(StorageKeyError, self.storage.get, i)


if __name__ == '__main__':
    unittest.main(verbosity=2)
