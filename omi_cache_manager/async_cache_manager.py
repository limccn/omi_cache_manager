import functools
import logging
import types
from abc import ABCMeta, abstractmethod

logger = logging.getLogger(__name__)


class AsyncCacheContext(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_cache_context(self):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError

    @abstractmethod
    def create_cache_context(self):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError

    @abstractmethod
    def destroy_cache_context(self):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError


class CacheBackend(AsyncCacheContext):
    __metaclass__ = ABCMeta

    @abstractmethod
    def clear(self):
        """
        "Proxy function for internal cache object."
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, *args, **kwargs):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError

    @abstractmethod
    def set(self, *args, **kwargs):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError

    @abstractmethod
    def add(self, *args, **kwargs):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, *args, **kwargs):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError

    @abstractmethod
    def delete_many(self, *args, **kwargs):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError

    @abstractmethod
    def get_many(self, *args, **kwargs):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError

    @abstractmethod
    def set_many(self, *args, **kwargs):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError

    @abstractmethod
    def execute(self, *args, **kwargs):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError


class SerializableCacheBackend(CacheBackend):
    __metaclass__ = ABCMeta

    @abstractmethod
    def load(self, *args, **kwargs):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError

    @abstractmethod
    def dump(self, *args, **kwargs):
        """
         "Proxy function for internal cache object."
        """
        raise NotImplementedError


class AsyncCacheManager:
    def __init__(self, app, cache_backend, config=None):
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

    def setup_app(self, app):
        # 为app增加cache_manager属性
        if isinstance(app, object) and hasattr(app, "state"):
            state = getattr(app, "state")
            if hasattr(state, "OMI_CACHE_MANAGER"):
                raise ValueError(
                    'Your context has bind to other Cache Manager, \
                    Can not set OMI_CACHE_MANAGER to an exist attr [app.state.OMI_CACHE_MANAGER]')
            else:
                setattr(state, "OMI_CACHE_MANAGER", self)

    def parse_backend_from_config(self, cache_backend, config):
        # 如果http_backend是str, 那么反射创建一个CacheBackend的instance
        if isinstance(cache_backend, str):
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
        return self.cache

    def handle_backend_cache_context(self):
        return self.cache.get_cache_context()

    async def create_backend_cache_context(self):
        return await self.async_method_call(
            self.cache.create_cache_context
        )

    async def destroy_backend_cache_context(self):
        return await self.async_method_call(
            self.cache.destory_cache_context
        )

    @classmethod
    async def async_method_call(cls, func, *args, **kwargs):
        func_type = type(func)
        if func_type in [types.MethodType, types.FunctionType]:
            func_call = functools.partial(func, *args, **kwargs)
            return await func_call()
        else:
            raise TypeError(f"Function {str(func)} must be FunctionType or MethodType")

    async def clear(self):
        return await self.async_method_call(
            self.cache.clear
        )

    async def get(self, *args, **kwargs):
        return await self.async_method_call(
            self.cache.get,
            *args,
            **kwargs
        )

    async def set(self, *args, **kwargs):
        return await self.async_method_call(
            self.cache.set,
            *args,
            **kwargs
        )

    async def add(self, *args, **kwargs):
        return await self.async_method_call(
            self.cache.add,
            *args,
            **kwargs
        )

    async def delete(self, *args, **kwargs):
        return await self.async_method_call(
            self.cache.delete,
            *args,
            **kwargs
        )

    async def delete_many(self, *args, **kwargs):
        return await self.async_method_call(
            self.cache.delete_many,
            *args,
            **kwargs
        )

    async def get_many(self, *args, **kwargs):
        return await self.async_method_call(
            self.cache.get_many,
            *args,
            **kwargs
        )

    async def set_many(self, *args, **kwargs):
        return await self.async_method_call(
            self.cache.set_many,
            *args,
            **kwargs
        )

    async def execute(self, *args, **kwargs):
        return await self.async_method_call(
            self.cache.execute,
            *args,
            **kwargs
        )
