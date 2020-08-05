"""
Copyright 2020 limc.cn All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import asyncio
from abc import ABCMeta, abstractmethod

from pydantic import RedisDsn

from ._decorators import async_method_in_loop
from .async_cache_manager import CacheBackend, CacheContext


class NullCacheBackend(CacheBackend):
    def __init__(self, config=None):
        """
        __init__构造函数，使用参数创建一个SimpleCacheBackend实例对象，，抽象类不能被实例化
            config - Backend配置相关的Dict，可以为None
        """
        super().__init__()

        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")
        self.config = config

        if config is not None:
            self.key_prefix = config.get('CACHE_KEY_PREFIX', str(self.__class__.__name__).upper())
        else:
            # 使用self.__class__.__name__做为prefix
            self.key_prefix = str(self.__class__.__name__).upper()

        pass

    def get_cache_context(self):
        """
        Implement function from CacheBackendContext interface, always return None.
        @See CacheBackendContext.get_cache_context
        """
        return None

    def create_cache_context(self):
        """
        Implement function from CacheBackendContext interface, always return None.
        @See CacheBackendContext.create_cache_context
        """
        return None

    async def destroy_cache_context(self):
        """
        Implement function from CacheBackendContext interface, always return None.
        @See CacheBackendContext.destroy_cache_context
        """
        # noop just wait
        return await asyncio.sleep(0.01)

    @async_method_in_loop
    def get(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface, always return None.
        @See CacheBackend.get
        """
        return None

    @async_method_in_loop
    def set(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface, always return True.
        @See CacheBackend.set
        """
        return True

    @async_method_in_loop
    def add(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface, always return True.
        @See CacheBackend.add
        """
        return True

    @async_method_in_loop
    def delete(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface, always return True.
        @See CacheBackend.delete
        """
        return True

    @async_method_in_loop
    def delete_many(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface, always return True.
        @See CacheBackend.delete_many
        """
        return True

    @async_method_in_loop
    def clear(self):
        """
        Implement function from CacheBackend interface, always return None.
        @See CacheBackend.clear
        """
        return None

    @async_method_in_loop
    def get_many(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface, always return None.
        @See CacheBackend.get_many
        """
        return None

    @async_method_in_loop
    def set_many(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface, always return True.
        @See CacheBackend.set_many
        """
        return True

    @async_method_in_loop
    def execute(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface, always return None.
        @See CacheBackend.execute
        """
        return None


class SimpleCacheDictContext(CacheContext):
    def __init__(self):
        self._cache_dict = dict({"": "", "*": ""})

    def __enter__(self):
        if not self._cache_dict:
            self._cache_dict = dict({"": "", "*": ""})
        return self._cache_dict

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    async def __aenter__(self):
        # NoOp
        await asyncio.sleep(0.1)
        if not self._cache_dict:
            self.create()
        return self._cache_dict

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # NoOp
        await asyncio.sleep(0.1)
        pass

    def destroy(self):
        """
        Implement function from CacheContext interface
        @See CacheContext.destroy
        """
        if not self._cache_dict:
            return
        self._cache_dict.clear()
        self._cache_dict = None

    def create(self):
        """
        Implement function from CacheContext interface
        @See CacheContext.create
        """
        self._cache_dict = dict({"": "", "*": ""})

    @property
    def cache_dict(self):
        return self._cache_dict


class SimpleCacheBackend(CacheBackend):
    def __init__(self, config=None):
        """
        __init__构造函数，使用参数创建一个SimpleCacheBackend实例对象，并返回
            config - Backend配置相关的Dict，可以为None
        """
        super().__init__()

        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")
        self.config = config
        self._cache_context = None
        if config is not None:
            self.key_prefix = config.get('CACHE_KEY_PREFIX', str(self.__class__.__name__).upper())
        else:
            # 使用self.__class__.__name__做为prefix
            self.key_prefix = str(self.__class__.__name__).upper()
        # setup
        self.setup_config(config)

    def make_key(self, key):
        """
        生成key，使用f"{self.key_prefix}{key}"
        """
        return f"{self.key_prefix}{key}"

    def setup_config(self, config=None):
        """
        从config配置backend
        """
        # do something to setup
        # create context
        if not self._cache_context:
            self.create_cache_context()

    def get_cache_context(self):
        """
        Implement function from CacheBackendContext interface.
        @See CacheBackendContext.get_cache_context
        """
        return self._cache_context

    def create_cache_context(self):
        """
        Implement function from CacheBackendContext interface.
        @See CacheBackendContext.create_cache_context
        """
        self._cache_context = SimpleCacheDictContext()

    async def destroy_cache_context(self):
        """
        Implement function from CacheBackendContext interface.
        @See CacheBackendContext.destroy_cache_context
        """
        if not self._cache_context:
            return
        self._cache_context.destroy()
        self._cache_context = None
        # noop just wait
        return await asyncio.sleep(0.01)

    def get_cache(self):
        with self.get_cache_context() as cache_dict:
            return cache_dict

    @async_method_in_loop
    def get(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface.
        @See CacheBackend.get
        """
        if len(args) == 1 and len(kwargs) == 0:
            key = self.make_key(args[0])
        elif len(args) == 0 and len(kwargs) == 1:
            key = self.make_key(kwargs["key"])
        else:
            raise TypeError("Too many or no key to get, args = %s kwargs= %s" % (str(args), str({**kwargs})))
        try:
            return self.get_cache().get(key)
        except Exception:
            raise KeyError("Get Key Error, key=%s" % key)

    @async_method_in_loop
    def set(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface.
        @See CacheBackend.set
        """
        # 筛选除["expire","pexpire","exist"]以外的key-val
        filter_kv = {k: v for k, v in kwargs.items() if k not in ["expire", "pexpire", "exist"]}
        cache = self.get_cache()
        if len(args) == 0:
            if len(filter_kv) == 0:
                raise TypeError("Mapping for set might missing, kwargs = %s" % str({**kwargs}))
            elif len(filter_kv) == 1:
                kv2update = {self.make_key(k): v for k, v in kwargs.items()}
                try:
                    cache.update(kv2update)
                except KeyError:
                    raise KeyError("Set Key Error, key=%s" % kv2update.keys)
            else:
                raise TypeError(
                    "Too many mappings to set, Use set_many method instead of set method, kwargs = %s" % str(
                        {**kwargs}))
        elif len(args) == 1:
            if isinstance(args[0], tuple):
                (key, value) = args[0]
                key = self.make_key(key)
            else:
                raise TypeError("Value is required to set key: %s, or paired tuple (key, value)" % str(args[0]))
            try:
                cache[key] = value
            except KeyError:
                raise KeyError("Set Key Error, key=%s" % key)
        elif len(args) == 2:
            key = self.make_key(args[0])
            value = args[1]
            try:
                cache[key] = value
            except KeyError:
                raise KeyError("Set Key Error, key=%s" % key)
        else:
            raise TypeError("Too many keys to set, Use set_many method instead of set method, keys = %s" % str(args))
        return True

    @async_method_in_loop
    def delete(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface.
        @See CacheBackend.delete
        """
        cache = self.get_cache()
        if len(args) == 1 and len(kwargs) == 0:
            key = self.make_key(args[0])
        elif len(args) == 0 and len(kwargs) == 1:
            key = self.make_key(kwargs["key"])
        else:
            raise TypeError("Too many or no key to delete, args = %s kwargs= %s" % (str(args), str({**kwargs})))
        try:
            # 查找并删除
            cache.get(key)
            cache.pop(key)
        except Exception:
            raise KeyError("Delete Key Error, key=%s" % key)
        return True

    @async_method_in_loop
    def get_many(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface.
        @See CacheBackend.get_many
        """
        results = []
        cache = self.get_cache()
        if len(args) == 0:
            raise TypeError("No keys for delete_many, args=%s" % str(args))
        for i in range(len(args)):
            key = self.make_key(args[i])
            try:
                val = cache.get(key)
            except KeyError:
                raise KeyError("Get Key Error, key=%s" % key)
            results.append(val)
        return results

    @async_method_in_loop
    def delete_many(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface.
        @See CacheBackend.delete_many
        """
        cache = self.get_cache()
        if len(args) == 0:
            raise TypeError("No keys for delete_many, keys=%s" % str(args))
        for i in range(len(args)):
            key = self.make_key(args[i])
            try:
                val = cache.get(key)
                if val:
                    cache.pop(key)
            except KeyError:
                raise KeyError("Delete Key Error, key=%s" % key)
        return True

    @async_method_in_loop
    def set_many(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface.
        @See CacheBackend.set_many
        """
        kv2update = {
            **{self.make_key(k): v for k, v in dict(args).items()},
            **{self.make_key(k): v for k, v in kwargs.items()},
        }
        try:
            cache = self.get_cache()
            if len(kv2update) > 0:
                cache.update(kv2update)
            else:
                raise TypeError("No keys for get_many, keys=%s" % kv2update.keys)
        except KeyError:
            raise KeyError("Set Keys Error, keys=%s" % kv2update.keys)
        except ValueError as ex:
            raise ValueError("Error while converting args to dictionary, set_many supports tuple, but not strings",
                             str(ex))
        return True

    async def add(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.add
        """
        # TODO add should return false when key is exist
        return await self.set(*args, **kwargs)

    async def execute(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.execute
        """
        if len(args) > 0:
            cmd = args[0]
            args_ex_cmd = args[1:]
        else:
            raise TypeError("Execute command can not empty")

        cmd = str.lower(cmd)
        if cmd == "get":
            return await self.get(*args_ex_cmd, **kwargs)
        elif cmd == "mget":
            return await self.get_many(*args_ex_cmd, **kwargs)
        elif cmd == "set":
            return await self.set(*args_ex_cmd, **kwargs)
        elif cmd == "mset":
            return await self.set_many(*args_ex_cmd, **kwargs)
        elif cmd == "del":
            return await self.delete(*args_ex_cmd, **kwargs)
        else:
            raise TypeError("Unimplemented command %s", cmd)

    @async_method_in_loop
    def clear(self):
        cache = self.get_cache()
        if len(cache) > 0:
            cache.clear()
        else:
            return True
        return True


class RedisContext(CacheContext):
    __metaclass__ = ABCMeta

    def __init__(self):
        self._conn_or_pool = None

    def __enter__(self):
        if not self._conn_or_pool:
            self.create()
        return self._conn_or_pool

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.destroy()

    async def __aenter__(self):
        if not self._conn_or_pool:
            await self.create()
        return self._conn_or_pool

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.destroy()

    @abstractmethod
    async def create(self):
        """
        Proxy function for internal cache object.
        @See CacheContext.create
        """

    @abstractmethod
    async def destroy(self):
        """
        Proxy function for internal cache object.
        @See CacheContext.destroy
        """


class RedisBackend(CacheBackend):
    __metaclass__ = ABCMeta

    def __init__(self, config=None):
        """
        __init__构造函数，使用参数创建一个RedisBackend实例对象，抽象类不能被实例化
            config - Backend配置相关的Dict，可以为None
        """
        super().__init__()
        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")
        self._redis_cache_context = None

        if config is not None:
            self.redis_scheme = config.get('CACHE_REDIS_SCHEME', 'redis')
            self.redis_host = config.get('CACHE_REDIS_HOST', 'localhost')
            self.redis_port = config.get('CACHE_REDIS_PORT', 6379)
            self.redis_user = config.get('CACHE_REDIS_USER', '')
            self.redis_password = config.get('CACHE_REDIS_PASSWORD', '')
            self.redis_db = config.get('CACHE_REDIS_DATABASE', '')
            # 默认使用self.__class__.__name__做为prefix
            self.key_prefix = config.get('CACHE_KEY_PREFIX', str(self.__class__.__name__).upper())
            self.redis_uri = config.get('CACHE_SCHEME_URI', None)
        else:
            self.redis_scheme = 'redis'
            self.redis_host = 'localhost'
            self.redis_port = 6379
            self.redis_user = ''
            self.redis_password = ''
            self.redis_db = ''
            # 使用self.__class__.__name__做为prefix
            self.key_prefix = str(self.__class__.__name__).upper()
            self.redis_uri = None

        self.setup_config(config)

    def setup_config(self, config=None):
        # do something to setup
        if self.redis_uri:
            pass
        else:
            self.redis_uri = RedisDsn.build(
                scheme=self.redis_scheme,
                host=self.redis_host,
                port=str(self.redis_port),
                user=self.redis_user,
                password=self.redis_password,
                path=f"/{str(self.redis_db) or ''}"
            )

        if not self._redis_cache_context:
            self.create_cache_context()

    def get_cache_context(self):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackendContext.get_cache_context
        """
        return self._redis_cache_context

    def create_cache_context(self):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackendContext.create_cache_context
        """
        self._redis_cache_context = RedisContext()

    async def destroy_cache_context(self):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackendContext.destroy_cache_context
        """
        if not self._redis_cache_context:
            return
        await self._redis_cache_context.destroy()
        self._redis_cache_context = None
        # just wait
        return await asyncio.sleep(0.01)

    @abstractmethod
    def clear(self):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackend.clear
        """

    @abstractmethod
    def get(self, *args, **kwargs):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackend.get
        """

    @abstractmethod
    def set(self, *args, **kwargs):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackend.set
        """

    @abstractmethod
    def add(self, *args, **kwargs):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackend.clear
        """

    @abstractmethod
    def delete(self, *args, **kwargs):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackend.clear
        """

    @abstractmethod
    def delete_many(self, *args, **kwargs):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackend.delete_many
        """

    @abstractmethod
    def get_many(self, *args, **kwargs):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackend.get_many
        """

    @abstractmethod
    def set_many(self, *args, **kwargs):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackend.set_many
        """

    @abstractmethod
    def execute(self, *args, **kwargs):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackend.execute
        """
