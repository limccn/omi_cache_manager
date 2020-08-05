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

import aioredis
from aioredis import ReplyError

from .backends import RedisBackend, RedisContext


class AIORedisContext(RedisContext):
    def __init__(self,
                 redis_uri,
                 timeout=3,
                 encoding="utf-8"):
        super().__init__()
        """
        __init__构造函数，使用参数创建一个ARedisContextPool实例对象，并返回
        :redis_uri - str default='localhost', Redis服务器地址
        :timeout - int or str default=None,连接Redis服务器的超市时间
        :encoding - str default=‘utf-8’, 连接Redis服务器使用的编码格式，默认使用utf-8
        """
        self.redis_uri = redis_uri
        self.timeout = timeout
        self.encoding = encoding

    async def __aenter__(self):
        if not self._conn_or_pool or self._conn_or_pool.closed:
            await self.create()
        return self._conn_or_pool

    async def create(self):
        """
        Proxy function for internal cache object.
        @See CacheContext.create
        """
        self._conn_or_pool = await aioredis.create_redis(
            address=self.redis_uri,
            timeout=self.timeout,
            encoding=self.encoding
        )

    async def destroy(self):
        """
        Proxy function for internal cache object.
        @See CacheContext.destroy
        """
        if not self._conn_or_pool:
            return
        self._conn_or_pool.close()
        await self._conn_or_pool.wait_closed()


class AIORedisContextPool(AIORedisContext):
    def __init__(self,
                 redis_uri,
                 timeout=3,
                 encoding="utf-8",
                 minsize=0,
                 maxsize=50):
        """
        __init__构造函数，使用参数创建一个ARedisContextPool实例对象，并返回
        :redis_uri - str default='localhost', Redis服务器地址
        :timeout - int or str default=None,连接Redis服务器的超市时间
        :encoding - str default=‘utf-8’, 连接Redis服务器使用的编码格式，默认使用utf-8
        :minsize - int default=0,Redis连接池的最小Size
        :maxsize - int default=50,Redis连接池的最大Size
        """
        super().__init__(redis_uri=redis_uri,
                         timeout=timeout,
                         encoding=encoding)
        self.minsize = minsize
        self.maxsize = maxsize

    async def create(self):
        """
        Proxy function for internal cache object.
        @See CacheContext.create
        """
        self._conn_or_pool = await aioredis.create_redis_pool(
            address=self.redis_uri,
            encoding=self.encoding,
            minsize=self.minsize,
            maxsize=self.maxsize,
            timeout=self.timeout
        )


class AIORedisBackend(RedisBackend):
    def __init__(self, config=None):
        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")

        if config is not None:
            # redis配置
            self.connection_timeout = config.get('CACHE_REDIS_CONNECTION_TIMEOUT', 3)
            # 连接池
            self.use_pool = config.get('CACHE_REDIS_USE_POOL', True)
            self.pool_minsize = config.get('CACHE_REDIS_POOL_MINSIZE', 3)
            self.pool_maxsize = config.get('CACHE_REDIS_POOL_MAXSIZE', 10)
            # encoding
            self.encoding = config.get('CACHE_REDIS_ENCODING', 'utf-8')
        else:
            self.connection_timeout = 3
            self.use_pool = True
            self.pool_minsize = 3
            self.pool_maxsize = 10
            self.encoding = 'utf-8'

        super().__init__(config=config)

    def create_cache_context(self):
        """
            覆盖父类的create_cache_context
            @See RedisBackend.create_cache_context
        """
        if self.use_pool:
            self._redis_cache_context = AIORedisContextPool(
                redis_uri=self.redis_uri,
                timeout=self.connection_timeout,
                encoding=self.encoding,
                minsize=self.pool_minsize,
                maxsize=self.pool_maxsize
            )
        else:
            self._redis_cache_context = AIORedisContext(
                redis_uri=self.redis_uri,
                timeout=self.connection_timeout,
                encoding=self.encoding,
            )

    def get_cache_context(self):
        """
        Implement function from CacheBackendContext interface
        @See CacheBackendContext.get_cache_context
        """
        return self._redis_cache_context

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

    def get_async_context(self):
        """
        Implement function from RedisBackend interface
        @See RedisBackend.get_async_context
        """
        return self.get_cache_context()

    def make_key(self, key):
        """
        生成key，使用f"{self.key_prefix}{key}"
        """
        return f"{self.key_prefix}{key}"

    async def get(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.get
        """
        if len(args) == 1 and len(kwargs) == 0:
            key = self.make_key(args[0])
        elif len(args) == 0 and len(kwargs) == 1:
            key = self.make_key(kwargs["key"])
        else:
            raise TypeError("Too many or no key to get, args = %s kwargs= %s" % (str(args), str({**kwargs})))
        async with self.get_async_context() as conn:
            return await conn.get(key)

    async def set(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.set
        """
        expire = kwargs.get("expire", 0)
        pexpire = kwargs.get("pexpire", 0)
        exist = kwargs.get("exist", None)

        # 筛选除["expire","pexpire","exist"]以外的key-val
        filter_kv = {k: v for k, v in kwargs.items() if k not in ["expire", "pexpire", "exist"]}
        if len(args) == 0:
            if len(filter_kv) == 0:
                raise TypeError("Mapping for set might missing, kwargs = %s" % str({**kwargs}))
            elif len(filter_kv) == 1:
                key = list(kwargs.keys())[0]
                value = kwargs[key]
                key = self.make_key(key)
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
        elif len(args) == 2:
            key = self.make_key(args[0])
            value = args[1]
        else:
            raise TypeError("Too many keys to set, Use set_many method instead of set method, keys = %s" % str(args))

        async with self.get_async_context() as conn:
            result = await conn.set(key=key,
                                    value=value,
                                    expire=expire,
                                    pexpire=pexpire,
                                    exist=exist)
        return result

    async def add(self, *args, **kwargs):
        kwargs["exist"] = "SET_IF_NOT_EXIST"
        return await self.set(*args, **kwargs)

    async def delete(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.delete
        """
        if len(args) == 1 and len(kwargs) == 0:
            key = self.make_key(args[0])
        elif len(args) == 0 and len(kwargs) == 1:
            key = self.make_key(kwargs["key"])
        else:
            raise TypeError("Too many or no key to delete, args = %s kwargs= %s" % (str(args), str({**kwargs})))
        async with self.get_async_context() as conn:
            result = await conn.delete(key=key)
        return result > 0

    async def delete_many(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.delete_many
        """
        keys = []
        for i in range(len(args)):
            key = self.make_key(args[i])
            keys.append(key)
        async with self.get_async_context() as conn:
            if len(keys) > 0:
                result = await conn.delete(*tuple(keys))
            else:
                # nothing to delete
                return True
        return result > 0

    async def get_many(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.get_many
        """
        keys = []
        for i in range(len(args)):
            key = self.make_key(args[i])
            keys.append(key)
        async with self.get_async_context() as conn:
            if len(keys) > 0:
                result = await conn.mget(*tuple(keys))
            else:
                raise TypeError("No keys for get_many, args=%s" % str(args))
        return result

    async def set_many(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.set_many
        """
        kv2update = {
            **{self.make_key(k): v for k, v in dict(args).items()},
            **{self.make_key(k): v for k, v in kwargs.items()},
        }
        async with self.get_async_context() as conn:
            if len(kv2update) > 0:
                result = await conn.mset(kv2update)
            else:
                raise TypeError("No keys for get_many, args=%s" % str(args))
        return result

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
        try:
            cmd = str.lower(cmd)
            if cmd == "get":
                result = await self.get(*args_ex_cmd, **kwargs)
            elif cmd == "mget":
                result = await self.get_many(*args_ex_cmd, **kwargs)
            elif cmd == "set":
                result = await self.set(*args_ex_cmd, **kwargs)
            elif cmd == "mset":
                result = await self.set_many(*args_ex_cmd, **kwargs)
            elif cmd == "del":
                result = await self.delete(*args_ex_cmd, **kwargs)
            elif cmd in ["ping", "quit", "bgsave", "dbsize", "time",
                         "info", "lastsave", "flushdb", "sync", "bgrewriteaof"]:
                # simple commands
                async with self.get_async_context() as conn:
                    result = await conn.execute(*args, **kwargs)
            else:
                result = await self.execute_key(*args, **kwargs)
            return result
        except ReplyError as ex:
            # ReplyError
            raise TypeError("Execute command type error,detail=%s" % str(ex))

    async def execute_key(self, *args, **kwargs):
        cmd = args[0]
        args_except_cmd = args[1:]
        args_to_execute = [cmd]
        if len(args_except_cmd) > 0:
            # 默认第一个参数为key
            key = self.make_key(args_except_cmd[0])
            args_to_execute.append(key)
            args_to_execute.extend(args_except_cmd[1:])
        else:
            raise TypeError("Too many or no key to execute, command = %s param =%s kwargs= %s"
                            % (str(cmd), str(*args_except_cmd), str({**kwargs})))

        async with self.get_async_context() as conn:
            cmd = str.lower(cmd)
            if cmd == "dump":
                result = await conn.dump(key)
            elif cmd == "incr":
                result = await conn.incr(key)
            elif cmd == "decr":
                result = await conn.decr(key)
            else:
                result = await conn.execute(*tuple(args_to_execute), **kwargs)
            return result

    async def clear(self):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.clear
        """
        async with self.get_async_context() as conn:
            keys = await conn.keys(self.make_key("*"))
            if len(keys) > 0:
                result = await conn.delete(*tuple(keys))
            else:
                # nothing to clear
                return True
        return result > 0
