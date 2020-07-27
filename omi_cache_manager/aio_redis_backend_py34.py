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

import aioredis

from .backends import RedisContext


class AIORedisContext(RedisContext):
    def __init__(self,
                 redis_uri,
                 timeout=3,
                 encoding="utf-8"):
        super().__init__()
        self.redis_uri = redis_uri
        self.timeout = timeout
        self.encoding = encoding

    async def conn(self):
        if not self._conn_or_pool or self._conn_or_pool.closed:
            await self.create()
        return self._conn_or_pool

    async def close(self, exc_type, exc_val, exc_tb):
        await self.destroy()

    async def create(self):
        self._conn_or_pool = await aioredis.create_redis(
            address=self.redis_uri,
            timeout=self.timeout,
            encoding=self.encoding
        )

    async def destroy(self):
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
        super().__init__(redis_uri=redis_uri,
                         timeout=timeout,
                         encoding=encoding)
        self.minsize = minsize
        self.maxsize = maxsize

    async def create(self):
        self._conn_or_pool = await aioredis.create_redis_pool(
            address=self.redis_uri,
            encoding=self.encoding,
            minsize=self.minsize,
            maxsize=self.maxsize,
            timeout=self.timeout
        )


def redis_context(wrapper):
    def _redis_context(func):
        async def __redis_context(*args, **kwargs):
            # wrapper为空，跳过
            if wrapper is None:
                return None
            # 获取连接
            conn = await wrapper.conn()
            # 执行
            result = await func(conn=conn, *args, **kwargs)
            # 释放连接
            await wrapper.close()
            return result

        return __redis_context

    return _redis_context
