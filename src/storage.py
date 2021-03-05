import time
from src.exceptions.storage_exceptions import *
from src.redis_pattern_matching import *
from twisted.internet import reactor
import random
import pickle


class Storage(object):
    """
    Class for keys and values storing.
    has ttl functionality.
    """
    def __init__(self, gc=False, file_prefix=None):
        """
        self.key_dict: dictionary for storing keys and values
        self.moe_dict: dictionary for storing moments of expiration of keys
        :param gc: enables garbage collector
        :param file_prefix: prefix of file names for saving/loading keys and moes,
            set None to disable saving
        """
        self._keys_dict = {}
        self._moe_dict = {}
        self.file_prefix = file_prefix
        if file_prefix:
            self.load()
        if gc:
            self.garbage_collector = StorageGarbageCollector(self)

    def set(self, key, value, moe=None, keep_moe=False, get=False):
        """
        Adds the key-value pair to the key_dict.
        Adds the moment of expiration to the moe_dict,
        if it's specified.
        :param key:
        :param value:
        :param moe: Moment of expiration. None if there is no constraint
        :param keep_moe: Keep previous moe for the key, if it has one
        :param get: return previous value of a key if it has one
        :return: None or previous value, if 'get' option is True
        """
        prev = None
        if get:
            prev = self._keys_dict.get(key)
        self._keys_dict[key] = value
        if not keep_moe:
            if moe is None:
                if key in self._moe_dict:
                    self._moe_dict.pop(key)
            else:
                self._moe_dict[key] = moe
        return prev

    def get(self, key):
        """
        Return the value of the key. Checks key
        expiration beforehand.
        :param key:
        :return:
        :exception StorageKeyError: no such key or it's expired
        """
        now = time.time()
        if key in self._moe_dict and \
                self._moe_dict[key] <= now:
            self._moe_dict.pop(key)
            self._keys_dict.pop(key)
        try:
            val = self._keys_dict[key]
        except KeyError:
            raise StorageKeyError(f'no key {key}')
        else:
            return val

    def delete(self, keys: list) -> int:
        """
        Delete a number of keys from storage,
        specified in the list. Checks expiration
        beforehand.
        :param keys: list of keys to delete
        :return: number of deleted keys
        """
        now = time.time()
        count = 0
        for key in keys:
            if key in self._keys_dict:
                self._keys_dict.pop(key)
                if key in self._moe_dict:
                    if self._moe_dict[key] > now:
                        count += 1
                    self._moe_dict.pop(key)
                else:
                    count += 1
        return count

    def keys(self, pattern: str) -> list:
        """
        Return all keys matching the pattern
        :param pattern:
        :return: list of keys that match the pattern
        :exception StoragePatternError: there is an error in the pattern
        """
        now = time.time()
        keys = []
        expired_keys = []
        # finding all matching keys, expired keys are stored separately
        for key in self._keys_dict.keys():
            if str_match_pattern_redis(str(key), pattern) == -1:
                if key in self._moe_dict and \
                  self._moe_dict[key] <= now:
                    expired_keys.append(key)
                else:
                    keys.append(key)
        for key in expired_keys:
            self._moe_dict.pop(key)
            self._keys_dict.pop(key)
        return keys

    def get_val_and_moe(self, key):
        """
        Get value and moe of a key. Raise KeyValue, is there is no such key
        or it's expired. Moe is None if key has no moe.
        :param key:
        :return: (value, moe)
        :exception StorageKeyError: no such key
        """
        val = self.get(key)
        return val, self._moe_dict.get(key)

    def set_moe(self, key, moe=None):
        """
        Set moe for a key or remove moe.
        :param key:
        :param moe: Moment of expiration. If None, moe is removed
        :return: None
        :exception StorageKeyError: no such key
        """
        if key not in self._keys_dict:
            raise StorageKeyError('no such key')
        elif moe is None:
            if key in self._moe_dict:
                self._moe_dict.pop(key)
        else:
            self._moe_dict[key] = moe

    def save(self):
        """
        Save keys and moes to disk
        :return:
        """
        if self.file_prefix:
            try:
                with open(self.file_prefix + '_keys.pkl', 'wb') as f:
                    pickle.dump(self._keys_dict, f, pickle.HIGHEST_PROTOCOL)
            except (OSError, pickle.PickleError) as err:
                raise StorageFileError("can't save keys")
            try:
                with open(self.file_prefix + '_moes.pkl', 'wb') as f:
                    pickle.dump(self._moe_dict, f, pickle.HIGHEST_PROTOCOL)
            except (OSError, pickle.PickleError) as err:
                raise StorageFileError("can't save keys' moes")

    def load(self):
        """
        Load keys and moes from disk
        :return:
        """
        if self.file_prefix:
            file_path = self.file_prefix + '_keys.pkl'
            try:
                with open(file_path, 'rb') as f:
                    self._keys_dict = pickle.load(f)
            # if no file was found, create empty dicts
            except FileNotFoundError:
                self._keys_dict = {}
                self._moe_dict = {}
            # if file is not accessible, raise an exception
            except IOError:
                raise StorageFileError(f"can't read {file_path}")
            except pickle.UnpicklingError:
                raise StorageFileError(f"can't unpickle {file_path}")
            # if keys were loaded, load moes
            else:
                file_path = self.file_prefix + '_moes.pkl'
                try:
                    with open(self.file_prefix + '_moes.pkl', 'rb') as f:
                        self._moe_dict = pickle.load(f)
                # both files must be present
                except FileNotFoundError:
                    raise StorageFileError(f"can't load moes, {file_path} does not exist")
                except IOError:
                    raise StorageFileError(f"can't read {file_path}")
                except pickle.UnpicklingError:
                    raise StorageFileError(f"can't unpickle {file_path} file")
                for key in self._moe_dict:
                    raise StorageFileError(f"found moe for a key {key} that does not exist")


class StorageGarbageCollector:
    """
    Primitive garbage collector for expired keys
    """
    def __init__(self, storage, call_interval=0.1):
        self.storage = storage
        self.base_call_interval = call_interval
        reactor.callLater(self.base_call_interval, self.expire_random)

    def expire_random(self):
        """
        Chooses 20 random keys from storage volatile keys
        and checks if any should be expired. If more than 25% of chosen
        keys are expired, chooses again.
        Calls itself later using twisted reactor with self.call_interval delay.
        :return:
        """
        if len(self.storage._moe_dict) > 0:
            check = True
            while check and len(self.storage._moe_dict):
                if len(self.storage._moe_dict) > 20:
                    keys_to_check = random.sample(list(self.storage._moe_dict), 20)
                else:
                    keys_to_check = list(self.storage._moe_dict)
                count = 0
                for key in keys_to_check:
                    if time.time() >= self.storage._moe_dict[key]:
                        self.storage._moe_dict.pop(key)
                        self.storage._keys_dict.pop(key)
                        count += 1
                if count < 5:
                    check = False
        reactor.callLater(self.base_call_interval, self.expire_random)
