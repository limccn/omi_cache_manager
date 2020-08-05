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
from typing import Type

from aredis import StrictRedis, StrictRedisCluster

from .backends import RedisBackend, RedisContext


class ARedisContext(RedisContext):
    def __init__(self,
                 cls_type: Type[StrictRedis] = StrictRedis,
                 host='localhost',
                 port=6379,
                 db=None,
                 password=None,
                 connect_timeout=None,
                 encoding='utf-8',
                 decode_responses=False
                 ):
        """
        __init__构造函数，使用参数创建一个ARedisContextPool实例对象，并返回
        :host - str default='localhost', Redis服务器地址
        :port - int default=6379, Redis服务器端口
        :db - int default=None, Redis服务器的DB编号，默认是None，使用0号数据库
        :password - str default=None,Redis服务器的密码
        :connect_timeout - int or str default=None,连接Redis服务器的超市时间
        :encoding - str default=‘utf-8’, 连接Redis服务器使用的编码格式，默认使用utf-8
        :decode_responses - bool default=False, 是否转编码redis的返回值，默认使用redis的编码
        """
        super().__init__()
        self._redis_cls_type = cls_type
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.connect_timeout = connect_timeout
        self.encoding = encoding
        self.decode_responses = decode_responses
        self._conn_or_pool = None

    def __enter__(self):
        if not self._conn_or_pool:
            self.create()
        return self._conn_or_pool

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def create(self):
        """
        Proxy function for internal cache object.
        @See CacheContext.create
        """
        self._conn_or_pool = self._redis_cls_type(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            connect_timeout=self.connect_timeout,
            encoding=self.encoding,
            decode_responses=self.decode_responses
        )

    def destroy(self):
        """
        Proxy function for internal cache object.
        @See CacheContext.destroy
        """
        if not self._conn_or_pool:
            return
        # NoOp just wait
        # await asyncio.sleep(0.01)
        # await self._conn_or_pool.disconnect_on_idle_time_exceeded()
        # self._conn_or_pool.connection_pool.disconnect()


class ARedisContextPool(ARedisContext):
    def __init__(self,
                 cls_type: Type[StrictRedis] = StrictRedis,
                 host='localhost',
                 port=6379,
                 db=None,
                 password=None,
                 connect_timeout=None,
                 encoding='utf-8',
                 decode_responses=False,
                 max_connections=None,
                 retry_on_timeout=False,
                 max_idle_time=0,
                 idle_check_interval=1
                 ):
        """
        __init__构造函数，使用参数创建一个ARedisContextPool实例对象，并返回
        :host - str default='localhost', Redis服务器地址
        :port - int default=6379, Redis服务器端口
        :db - int default=None, Redis服务器的DB编号，默认是None，使用0号数据库
        :password - str default=None,Redis服务器的密码
        :connect_timeout - int or str default=None,连接Redis服务器的超市时间
        :encoding - str default=‘utf-8’, 连接Redis服务器使用的编码格式，默认使用utf-8
        :decode_responses - bool default=False, 是否转编码redis的返回值，默认使用redis的编码
        :max_connections - int default=None, 连接池最大连接数
        :retry_on_timeout - bool default=False,  超时后最大重试次数
        :max_idle_time - int default=0,  连接的最大空闲时间
        :idle_check_interval - int default=1, 检测连接的最大空闲的间隔
        """
        super().__init__(
            cls_type=cls_type,
            host=host,
            port=port,
            db=db,
            password=password,
            connect_timeout=connect_timeout,
            encoding=encoding,
            decode_responses=decode_responses
        )
        self.max_connections = max_connections
        self.retry_on_timeout = retry_on_timeout
        self.max_idle_time = max_idle_time
        self.idle_check_interval = idle_check_interval

    def create(self):
        """
        Proxy function for internal cache object.
        @See CacheContext.create
        """
        self._conn_or_pool = self._redis_cls_type(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            connect_timeout=self.connect_timeout,
            encoding=self.encoding,
            decode_responses=self.decode_responses,
            max_connections=self.max_connections,
            retry_on_timeout=self.retry_on_timeout,
            max_idle_time=self.max_idle_time,
            idle_check_interval=self.idle_check_interval
        )


class ARedisBackend(RedisBackend):
    def __init__(self, config=None):
        """
        __init__构造函数，使用参数创建一个ARedisBackend实例对象，并返回
            config - Backend配置相关的Dict，可以为None
        """
        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")

        if config is not None:
            # redis配置
            self.connection_timeout = config.get('CACHE_REDIS_CONNECTION_TIMEOUT', None)
            self.decode_responses = config.get('CACHE_REDIS_DECODE_RESPONSES', True)
            # 连接池
            self.use_pool = config.get('CACHE_REDIS_USE_POOL', True)
            self.use_cluster = config.get('CACHE_REDIS_USE_CLUSTER', False)
            # self.pool_minsize = config.get('CACHE_REDIS_POOL_MINSIZE',1)
            self.pool_maxsize = config.get('CACHE_REDIS_POOL_MAXSIZE', None)
            self.max_idle_time = config.get('CACHE_REDIS_MAX_IDLE_TIME', 0)
            self.retry_on_timeout = config.get('CACHE_REDIS_RETRY_ON_TIMEOUT', False)
            self.idle_check_interval = config.get('CACHE_REDIS_IDLE_CHECK_INTERVAL', 1)
            # encoding
            self.encoding = config.get('CACHE_REDIS_ENCODING', 'utf-8')
        else:
            self.connection_timeout = None
            self.decode_responses = True
            self.use_pool = True
            self.use_cluster = False
            self.pool_maxsize = None
            self.max_idle_time = 0
            self.retry_on_timeout = False
            self.idle_check_interval = 1
            self.encoding = 'utf-8'

        super().__init__(config=config)

    def create_cache_context(self):
        """
            Implement function from RedisBackend interface
            覆盖父类的create_cache_context
            @See RedisBackend.create_cache_context
        """
        # only 
        if self.use_cluster:
            instance_type = StrictRedisCluster
        else:
            instance_type = StrictRedis

        # only
        if self.use_pool:
            self._redis_cache_context = ARedisContextPool(
                cls_type=instance_type,
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                connect_timeout=self.connection_timeout,
                encoding=self.encoding,
                decode_responses=self.decode_responses,
                max_connections=self.pool_maxsize,
                retry_on_timeout=self.retry_on_timeout,
                max_idle_time=self.max_idle_time,
                idle_check_interval=self.idle_check_interval
            )
        else:
            self._redis_cache_context = ARedisContext(
                cls_type=instance_type,
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                connect_timeout=self.connection_timeout,
                encoding=self.encoding,
                decode_responses=self.decode_responses,
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
        self._redis_cache_context.destroy()
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
        with self.get_async_context() as conn:
            return await conn.get(key)

    async def set(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.set
        """
        expire = kwargs.get("expire", None)
        pexpire = kwargs.get("pexpire", None)

        exist = kwargs.get("exist", None)
        arg_xx = (exist is "SET_IF_EXIST")
        arg_nx = (exist is "SET_IF_NOT_EXIST")

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

        with self.get_async_context() as conn:
            result = await conn.set(
                key,
                value,
                ex=expire,
                px=pexpire,
                nx=arg_nx,
                xx=arg_xx
            )
        return result is True

    async def add(self, *args, **kwargs):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.add
        """
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
        with self.get_async_context() as conn:
            result = await conn.delete(key)
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
        with self.get_async_context() as conn:
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
        with self.get_async_context() as conn:
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
        with self.get_async_context() as conn:
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
                with self.get_async_context() as conn:
                    result = await conn.execute_command(*args, **kwargs)
            else:
                result = await self.execute_key(*args, **kwargs)
            return result
        except Exception as ex:
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

        with self.get_async_context() as conn:
            cmd = str.lower(cmd)
            if cmd == "dump":
                result = await conn.dump(key)
            elif cmd == "incr":
                result = await conn.incr(key)
            elif cmd == "decr":
                result = await conn.decr(key)
            else:
                result = await conn.execute_command(*tuple(args_to_execute), **kwargs)
            return result

    async def clear(self):
        """
        Implement function from CacheBackend interface
        @See CacheBackend.clear
        """
        with self.get_async_context() as conn:
            keys = await conn.keys(self.make_key("*"))
            if len(keys) > 0:
                result = await conn.delete(*tuple(keys))
            else:
                # nothing to clear
                return True
        return result > 0
