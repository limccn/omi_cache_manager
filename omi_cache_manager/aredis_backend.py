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
        pass
        # if not self._conn_or_pool:
        #     return
        # #await self._conn_or_pool.disconnect_on_idle_time_exceeded()
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
        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")
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

        super().__init__(config)

    def create_cache_context(self):
        """
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

    def get_async_context(self):
        return self.get_cache_context()

    def make_key(self, key):
        return f"{self.key_prefix}{key}"

    async def get(self, *args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            key = self.make_key(args[0])
        elif len(args) == 0 and len(kwargs) == 1:
            key = self.make_key(kwargs["key"])
        else:
            raise TypeError("Too many or no key to get, args = %s kwargs= %s" % (str(args), str({**kwargs})))
        with self.get_async_context() as conn:
            return await conn.get(key)

    async def set(self, *args, **kwargs):

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
        kwargs["exist"] = "SET_IF_NOT_EXIST"
        return await self.set(*args, **kwargs)

    async def delete(self, *args, **kwargs):
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
        if len(args) > 0:
            cmd = args[0]
            args_ex_cmd = args[1:]
        else:
            raise TypeError("Execute command can not empty")
        try:
            with self.get_async_context() as conn:
                if str.lower(cmd) == "get":
                    result = await self.get(*args_ex_cmd, **kwargs)
                elif str.lower(cmd) == "mget":
                    result = await self.get_many(*args_ex_cmd, **kwargs)
                elif str.lower(cmd) == "set":
                    result = await self.set(*args_ex_cmd, **kwargs)
                elif str.lower(cmd) == "mset":
                    result = await self.set_many(*args_ex_cmd, **kwargs)
                else:
                    result = await conn.execute(*args, **kwargs)
        except Exception as ex:
            raise TypeError("Execute command type error,detail=%s" % str(ex))
        return result

    async def clear(self):
        with self.get_async_context() as conn:
            keys = await conn.keys(self.make_key("*"))
            if len(keys) > 0:
                result = await conn.delete(*tuple(keys))
            else:
                # nothing to clear
                return True
        return result > 0
