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

import functools
import logging
import types
from abc import ABCMeta, abstractmethod

logger = logging.getLogger(__name__)


class CacheContext(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def create(self):
        """
        创建缓存的Context上下文中的缓存对象
        """

    @abstractmethod
    def destroy(self):
        """
        销毁缓存的Context上下文中的缓存对象
        """


class CacheBackendContext(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_cache_context(self):
        """
        Proxy function for internal cache context object.
        获取当前缓存的Context上下文对象，对于已创建完成的Context，返回相应Context对象引用
        """

    @abstractmethod
    def create_cache_context(self):
        """
        Proxy function for internal cache context object.
        创建缓存的Context上下文对象，对于异步Async的backend实现，支持使用同步Sync和异步Async方式完成调用，习惯上由Backend自行创建。
        """

    @abstractmethod
    def destroy_cache_context(self):
        """
        Proxy function for internal cache context object.
        销毁缓存的Context上下文对象，对于异步Async的backend实现，应该使用异步方式完成调用，否则会出现缓存无法被销毁的情况.
        ```
        await cache.destroy_cache_context()
        ```
        """


class CacheBackend(CacheBackendContext):
    __metaclass__ = ABCMeta

    @abstractmethod
    def clear(self):
        """
        清除缓存Context中所有的符合条件的key-value对象，删除通配符合格式为`*`的key和value
        如果使用了CACHE_KEY_PREFIX配置属性，则使用通配符方式清除所有符合格式为`{CACHE_KEY_PREFIX}*`的key和value
        注意：本操作会删除一个或多个key-value对象，且不可恢复，使用时请注意
        使用demo举例
        ```
        cache.clear()
        ```
        
        """

    @abstractmethod
    def get(self, *args, **kwargs):
        """
        获取一个key的value值
        :* - any, 使用key传入参数，支持使用参数列表("key")，参数mapping(key="key")
        :key - str or any repr, 需要获取value的key值，有且只能使用一个
        使用demo举例
        ```
        cache.get("foo_key")
        cache.get(key="foo_key")
        ```
        以下操作将抛出异常
        ```
        cache.get("foo_key","foo_key1","foo_key2")
        cache.get(key="foo_key","foo_key")
        ```
        以下在不同的Backend下可能会抛出异常
        ```
        cache.get("*")  #通配符，字典缓存有效，redis等无效
        cache.get('') # empty string，字典缓存有效，redis等无效
        cache.get(None) # Nothing，字典缓存无效，redis等有效
        ```
        """

    @abstractmethod
    def set(self, *args, **kwargs):
        """
        设置一个value到缓存并指定相应的key
        :* - any, 使用key,value的传入参数，支持使用参数列表("key","value")，参数mapping(key="key",value="value")，参数tuple方式(("key","value"))
        :key - str or any repr, 需要设置的key值，有且只能使用一个
        :value - any , 需要设置value的值，有且只能使用一个
        :expire - int  有效期，以秒为单位，具体作用可以参考Redis中关于`EXPIRE`参数的描述
        :pexpire - int 有效期，以毫秒为单位，具体作用可以参考Redis中关于`PEXPIRE`参数的描述
        :exist - str 可选值 "SET_IF_EXIST", "SET_IF_NOT_EXIST"
            "SET_IF_EXIST"，如果存在则进行set操作，具体作用可以参考Redis中关于`XX`参数的描述
            "SET_IF_NOT_EXIST"，如果不存在才会进行set操作，否则不操作,具体作用可以参考Redis中关于`NX`参数的描述
        使用demo举例
        ```
        cache.set("foo","bar")
        cache.set(foo="bar")
        cache.set(key="foo",value="bar")
        cache.set(("key","foo")) #tuple
        ```
        以下操作将抛出异常
        ```
        cache.set("foo","bar","key","val") 
        cache.set(key="foo",value="bar","key","val")
        ```
        以下在不同的Backend下可能会抛出异常
        ```
        cache.set("*","foobar")  #通配符，字典缓存有效，redis等无效
        cache.set("","foobar") # empty string，字典缓存有效，redis等无效
        cache.set(None,"foobar") # Nothing，字典缓存无效，redis等无效
        """

    @abstractmethod
    def add(self, *args, **kwargs):
        """
        增加一个value到缓存并指定相应的key，操作类似CacheBackend.set，但是增加了存在性限制，设置的key必须在cache中不存在才能完成add操作
        :* - any, 可以key,value的传入参数，支持使用参数列表("key","value")，
            参数mapping(key="key",value="value")，参数tuple方式(("key","value"))
        :key - str or any repr, 需要设置的key值，有且只能使用一个
        :value - any , 需要设置value的值，有且只能使用一个
        :expire - int  有效期，以秒为单位，具体作用可以参考Redis中关于`EXPIRE`参数的描述
        :pexpire - int 有效期，以毫秒为单位，具体作用可以参考Redis中关于`PEXPIRE`参数的描述
        :exist - 默认固定为"SET_IF_NOT_EXIST"，参数不建议使用

        使用demo举例
        ```
        cache.add("foo","bar")
        cache.add(foo="bar")
        cache.add(key="foo",value="bar")
        cache.set(("key","foo")) #tuple
        ```
        以下操作将抛出异常
        ```
        cache.add("foo","bar","key","val") 
        cache.add(foo="bar","key","val")
        cache.set(("key","foo"),("tuple","tuple")) #tuple
        # 重复add的情况
        cache.add(foo="bar1")
        cache.add(foo="bar2")
        cache.set(("foo","foo"),("foo","tuple")) #tuple
        ```
        以下在不同的Backend下可能会抛出异常
        ```
        cache.add("*","foobar")  #通配符，字典缓存有效，redis等无效
        cache.add("","foobar") # empty string，字典缓存有效，redis等无效
        cache.add(None,"foobar") # Nothing，字典缓存无效，redis等无效

        @See CacheBackend.set

        """

    @abstractmethod
    def delete(self, *args, **kwargs):
        """
        删除一个key的value值.
        :* - any, 使用key传入参数，支持使用参数列表("key")，参数mapping(key="key")
        :key - str or any repr, 需要删除value的key值，有且只能使用一个
        使用demo举例
        ```
        cache.delete("foo")
        cache.delete(key="foo")
        ```
        以下操作将抛出异常
        ```
        cache.delete("foo","foo1","foo2")
        cache.delete(key="foo","foo")
        ```
        以下在不同的Backend下可能会抛出异常
        ```
        cache.delete("*")  #通配符，字典缓存有效但是最多删除1个，redis等有效，将会删除全部的key
        cache.delete('') # empty string，字典缓存有效，redis等无效
        cache.delete(None) # Nothing，字典缓存无效，redis等有效
        ```
        """

    @abstractmethod
    def delete_many(self, *args, **kwargs):
        """
        删除一个或多个key的value值
        :* - any, 使用key传入参数，只支持使用参数列表("key1","key2","key3"),需要删除的key值,可用使用多个.
        使用demo举例
        ```
        cache.delete_many("foo_key")
        cache.delete_many("foo_key","foo_key1","foo_key2")
        ```
        以下操作将抛出异常
        ```
        cache.delete_many(key='foo',"foo_key","foo_key1","foo_key2")
        ```
        以下在不同的Backend下可能会抛出异常
        ```
        cache.delete_many("*","%")  #通配符，字典缓存有效但是最多删除1个，redis等有效，将会删除全部的key
        cache.delete_many('',' ') # empty string，字典缓存有效，redis等无效
        cache.delete_many(None) # Nothing，字典缓存无效，redis等有效
        ```
        """

    @abstractmethod
    def get_many(self, *args, **kwargs):
        """
        获取一个多个key的value值
        注意：在不同backend下使用get_many，返回的结果顺序不一定是传入参数的顺序。
        :* - any, 使用key传入参数，只支持使用参数列表("key1","key2","key3")，需要获取的key值,可用使用多个.
        使用demo举例
        ```
        cache.get_many("foo")
        cache.get_many("foo","foo1","foo2")
        ```
        以下操作将抛出异常
        ```
        cache.get_many(key="foo","foo")
        ```
        以下在不同的Backend下可能会抛出异常
        ```
        cache.get_many("*","%")   #通配符，字典缓存有效，redis等有效
        cache.get_many('',' ') # empty string，字典缓存有效，redis等有效
        cache.get_many(None) # Nothing，字典缓存无效，redis等有效
        ```
        """

    @abstractmethod
    def set_many(self, *args, **kwargs):
        """
        设置一个或多个value到缓存并指定相应的key
        :* - any, 可以key,value的传入参数，支持使用参数列表("key","value")，
            参数mapping(key="key",value="value")，参数tuple方式(("key","value"))
            如果要适配多种backend，在set_many注意不要混用
        使用demo举例
        ```
        cache.set_many(foo="bar")
        cache.set_many(key="foo",value="bar",value="aa")
        cache.set_many(("key1","foo"),("key2","bar"),("key3","foobar")) #tuple
        ```
        以下操作将抛出异常
        ```
        cache.set_many("foo","bar","key","val") 
        cache.set_many(key="foo",value="bar","key","val")
        cache.set_many(("key1","foo"),key="foo",value="bar")
        ```
        以下在不同的Backend下可能会抛出异常
        ```
        cache.set_many("*","foobar")  #通配符，字典缓存有效，redis等无效
        cache.set_many("","foobar") # empty string，字典缓存有效，redis等无效
        cache.set_many(None,"foobar") # Nothing，字典缓存无效，redis等无效
        """

    @abstractmethod
    def execute(self, *args, **kwargs):
        """
        使用low-level api执行相应的缓存命令，args[0] 必须是指令字符
        其中的GET/MGET和SET/MSET命令会相应转换为get/get_many和set/set_many操作完成,相应操作参考
        @See CacheBackend.get, CacheBackend.set, CacheBackend.get_many, CacheBackend.set_many
        其他类型操作的具体可以参考Backend各自实现。
        
        使用demo举例
        ```
        cache.execute("GET",key="foobar")
        cache.execute("SET",key="foobar",value="foo")
        cache.execute("MGET","foo","bar","aa")
        cache.execute("MSET",("key1","foo"),("key2","bar"),("key3","foobar")) #tuple

        """


class SerializableCacheBackend(CacheBackend):
    __metaclass__ = ABCMeta

    @abstractmethod
    def load(self, *args, **kwargs):
        """
        从指定位置或加载数据到缓存

        :source - str Load操作的输入目标
        :type - str, 数据的类型格式，例如json，csv等
        :* - any, 用于数据Load的参数 

        使用demo举例
        ```
        cache.load(source=‘/tmp/path’,type='json')    
        ```
        """

    @abstractmethod
    def dump(self, *args, **kwargs):
        """
        将缓存dump到指定位置
        :target - str Dump操作的输出目标
        :type - str, 数据的类型格式，例如json，csv等
        :* - any, 用于数据dump的参数 

        使用demo举例
        ```
        cache.dump(target=‘/tmp/path’,type='json') 
        ```
        """


class AsyncCacheManager:

    def __init__(self, app, cache_backend, config=None):
        """
        __init__构造函数，使用参数创建一个AsyncCacheManager实例对象，并返回
            app - None or Any,可以为None，如果设置了app context上下文,当前对象的引用将被设置到`app.state.OMI_CACHE_MANAGER`
            cache_backend - Union[str, CacheBackend]，Backend的实现类，需要继承CacheBackend接口
                可以使用str方式传入class名称，构造函数会根据str查找并自动解析响应的CacheBackend
                同时，也可以使用的backend别名方式完成backend的设置,不区分大小写
                传入"null_cache" 或者 "NullCacheBackend" 会使用 "omi_cache_manager.backends.NullCacheBackend"
                传入"simple_cache" 或者 "SimpleCacheBackend" 会使用"omi_cache_manager.backends.SimpleCacheBackend"
                传入"aioredis" 或者 "AIORedisBackend" 会使用"omi_cache_manager.aio_redis_backend.AIORedisBackend"
                传入"aredis"或者 "ARedisBackend" 会使用"omi_cache_manager.aredis_backend.ARedisBackend"

        """
        if not (config is None or isinstance(config, dict)):
            raise ValueError("`config` must be an instance of dict or None")
        self.config = config
        # 保留app的引用
        self._app_ref = app
        # 设置app
        self.setup_app(app)
        # 设置backend
        cache_backend_instance = self.parse_backend_from_config(cache_backend, config)
        self.cache_backend_name = cache_backend_instance.__class__.__name__
        self.cache = cache_backend_instance

    @property
    def app_ref(self):
        return self._app_ref

    def setup_app(self, app):
        """
        关联manager与app context上下文,当前manager对象的引用将被设置到`app.state.OMI_CACHE_MANAGER`
        """
        # 为app增加cache_manager属性
        if isinstance(app, object) and hasattr(app, "state"):
            state = getattr(app, "state")
            if state is not None and isinstance(state, object):
                if hasattr(state, "OMI_CACHE_MANAGER"):
                    raise ValueError(
                        'Your context has bind to other Cache Manager, \
                        Can not set OMI_CACHE_MANAGER to an exist attr [app.state.OMI_CACHE_MANAGER]')
                else:
                    app.state.OMI_CACHE_MANAGER = self
            else:
                pass

    def parse_backend_from_config(self, cache_backend, config):
        """
        配置当前manager的cache backend的实例
        """
        # 如果http_backend是str, 那么反射创建一个CacheBackend的instance
        if isinstance(cache_backend, str):
            cache_backend_lower = cache_backend.lower()
            # alias
            if cache_backend_lower in ["null_cache", "nullcachebackend"]:
                cache_backend = "omi_cache_manager.backends.NullCacheBackend"
            elif cache_backend_lower in ["simple_cache", "simplecachebackend"]:
                cache_backend = "omi_cache_manager.backends.SimpleCacheBackend"
            elif cache_backend_lower in ["aioredis", "aioredisbackend"]:
                cache_backend = "omi_cache_manager.aio_redis_backend.AIORedisBackend"
            elif cache_backend_lower in ["aredis", "aredisbackend"]:
                cache_backend = "omi_cache_manager.aredis_backend.ARedisBackend"
            else:
                pass
            name = cache_backend.split('.')
            used = name.pop(0)
            try:
                found = __import__(used)
                # 查找模块下同名class_meta
                for frag in name:
                    used += '.' + frag
                    try:
                        # 使用getattr方式获取type
                        found = getattr(found, frag)
                    except AttributeError:
                        # 使用__import__导入type
                        __import__(used)
                        found = getattr(found, frag)
                # 实例化instance
                cache_backend_instance = found(config=config)
            except ImportError:
                raise ValueError('Cannot resolve cache_backend type %s' % cache_backend)
        else:
            cache_backend_instance = cache_backend
        return cache_backend_instance

    @property
    def cache_backend(self):
        """
        获取当前manager的cache backend实例引用
        """
        return self.cache

    def handle_backend_cache_context(self):
        """
        Proxy function for internal cache context object.
        代理cache context的get_cache_context
        """
        return self.cache.get_cache_context()

    def create_backend_cache_context(self):
        """
        Proxy function for internal cache context object.
        代理cache context的create_cache_context，使用异步方式调用
        """
        return self.cache.create_cache_context()

    async def destroy_backend_cache_context(self):
        """
        Proxy function for internal cache context object.
        代理cache context的destroy_cache_context，使用异步方式调用
        """
        return await self.async_method_call(
            self.cache.destroy_cache_context
        )

    @classmethod
    async def async_method_call(cls, func, *args, **kwargs):
        """
        将同步对象方法转换为异步Generator方式执行，对于异步方法，不改变其执行方式。
        """
        func_type = type(func)
        if func_type in [types.MethodType, types.FunctionType]:
            func_call = functools.partial(func, *args, **kwargs)
            return await func_call()
        else:
            raise TypeError(f"Function {str(func)} must be FunctionType or MethodType")

    async def clear(self):
        """
        Proxy function for internal cache object.
        @See CacheBackend.clear
        """
        return await self.async_method_call(
            self.cache.clear
        )

    async def get(self, *args, **kwargs):
        """
        Proxy function for internal cache object.
        @See CacheBackend.get
        """
        return await self.async_method_call(
            self.cache.get,
            *args,
            **kwargs
        )

    async def set(self, *args, **kwargs):
        """
        Proxy function for internal cache object.
        @See CacheBackend.set
        """
        return await self.async_method_call(
            self.cache.set,
            *args,
            **kwargs
        )

    async def add(self, *args, **kwargs):
        """
        Proxy function for internal cache object.
        @See CacheBackend.add
        """
        return await self.async_method_call(
            self.cache.add,
            *args,
            **kwargs
        )

    async def delete(self, *args, **kwargs):
        """
        Proxy function for internal cache object.
        @See CacheBackend.delete
        """
        return await self.async_method_call(
            self.cache.delete,
            *args,
            **kwargs
        )

    async def delete_many(self, *args, **kwargs):
        """
        Proxy function for internal cache object.
        @See CacheBackend.delete_many
        """
        return await self.async_method_call(
            self.cache.delete_many,
            *args,
            **kwargs
        )

    async def get_many(self, *args, **kwargs):
        """
        Proxy function for internal cache object.
        @See CacheBackend.get_many
        """
        return await self.async_method_call(
            self.cache.get_many,
            *args,
            **kwargs
        )

    async def set_many(self, *args, **kwargs):
        """
        Proxy function for internal cache object.
        @See CacheBackend.set_many
        """
        return await self.async_method_call(
            self.cache.set_many,
            *args,
            **kwargs
        )

    async def execute(self, *args, **kwargs):
        """
        Proxy function for internal cache object.
        @See CacheBackend.execute
        """
        return await self.async_method_call(
            self.cache.execute,
            *args,
            **kwargs
        )
