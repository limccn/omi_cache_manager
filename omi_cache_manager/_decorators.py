import asyncio
import functools
from functools import wraps


def async_method_in_loop(func):
    @wraps(func)
    async def async_method_wrapper(*args, **kwargs):
        future = asyncio.get_event_loop().run_in_executor(None,
                                                          functools.partial(
                                                              func,
                                                              *args,
                                                              **kwargs
                                                          )
                                                          )
        return await future

    return async_method_wrapper


def cached(app, cache):
    def decorator(func):
        @functools.wraps(func)
        async def _inner(*args, **kwargs):
            key = func.__name__
            res = await cache.get(key, (args, kwargs))
            if res:
                print('using cache: {}'.format(res))
            else:
                print('cache miss')
                res = func(*args, **kwargs)
                await cache.set(key, res, (args, kwargs))
            return res

        return _inner

    return decorator
