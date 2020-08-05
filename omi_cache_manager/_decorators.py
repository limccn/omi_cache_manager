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
import functools
from functools import wraps


def async_method_in_loop(func):
    """
    Decorator for make Sync method call to Async
    """

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

# def cached(app, cache):
#     """
#     Decorator cache a function/method return to cache manger
#     """
#
#     def decorator(func):
#         @functools.wraps(func)
#         async def _inner(*args, **kwargs):
#             key = func.__name__
#             res = await cache.get(key, (args, kwargs))
#             if res:
#                 print('using cache: {}'.format(res))
#             else:
#                 print('cache miss')
#                 res = func(*args, **kwargs)
#                 await cache.set(key, res, (args, kwargs))
#             return res
#
#         return _inner
#
#     return decorator
