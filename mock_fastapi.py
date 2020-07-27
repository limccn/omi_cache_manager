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

import uvicorn
from fastapi import FastAPI, Depends
from fastapi.responses import JSONResponse

from omi_cache_manager import AsyncCacheManager

app = FastAPI()

# ==============================Demo for AsyncCacheManager==================================

ENV_INTEGRATION = True

if ENV_INTEGRATION:
    # cache = AsyncCacheManager(
    #     app, # None
    #     cache_backend = "omi_cache_manager.aio_redis_backend.AIORedisBackend",
    #     config={
    #         "CACHE_REDIS_SCHEME" : "redis",
    #         "CACHE_REDIS_HOST" : "192.168.201.169",
    #         "CACHE_REDIS_PORT" : "6379",
    #         "CACHE_REDIS_USER" : "user",
    #         "CACHE_REDIS_PASSWORD" : "",
    #         "CACHE_REDIS_DATABASE" : 8,

    #         'CACHE_REDIS_CONNECTION_TIMEOUT':3,
    #         'CACHE_REDIS_DECODE_RESPONSE':True,
    #         'CACHE_REDIS_ENCODING':'utf-8',

    #         'CACHE_REDIS_USE_POOL':True,
    #         'CACHE_REDIS_POOL_MINSIZE':1,
    #         'CACHE_REDIS_POOL_MAXSIZE':50,

    #         "CACHE_KEY_PREFIX":"MOCK_REDIS_INTEGRATION_TEST:"
    #     }
    # )

    cache = AsyncCacheManager(
        app,
        cache_backend="omi_cache_manager.aredis_backend.ARedisBackend",
        config={
            "CACHE_REDIS_SCHEME": "redis",
            "CACHE_REDIS_HOST": "192.168.201.169",
            "CACHE_REDIS_PORT": 6379,
            "CACHE_REDIS_PASSWORD": "",
            "CACHE_REDIS_DATABASE": 8,

            'CACHE_REDIS_CONNECTION_TIMEOUT': 3,
            'CACHE_REDIS_ENCODING': 'utf-8',

            'CACHE_REDIS_USE_POOL': True,
            'CACHE_REDIS_POOL_MINSIZE': 1,  # no effect
            'CACHE_REDIS_POOL_MAXSIZE': 50,

            'CACHE_REDIS_USE_CLUSTER': False,  # for cluster not tested
            'CACHE_REDIS_MAX_IDLE_TIME': 0,
            'CACHE_REDIS_RETRY_ON_TIMEOUT': False,
            'CACHE_REDIS_IDLE_CHECK_INTERVAL': 1,

            "CACHE_KEY_PREFIX": "A_REDIS_MANAGER_UNIT_TEST_POOL:"
        }
    )
else:
    cache = AsyncCacheManager(
        app,  # None
        cache_backend="omi_cache_manager.backends.SimpleCacheBackend",
        config={
            "CACHE_KEY_PREFIX": "MOCK_SIMPLE_INTEGRATION_TEST:"
        }
    )


# Depends的写法,使用manger
async def depends_cache_manager():
    return cache


# Depends的写法，使用backend，实现更高级的缓存功能
async def depends_cache_backend():
    return cache.cache_backend


@app.get("/mock/cache/{key}")
async def cache_get_key(key, cache_manager=Depends(depends_cache_manager)):
    value = await cache_manager.get(key)
    if value:
        return JSONResponse(
            status_code=200,
            content={
                "code": 100,
                "message": "success",
                "detail": {
                    "key": key,
                    "value": value
                }
            })
    else:
        return JSONResponse(
            status_code=404,
            content={
                "code": 104,
                "message": "",
                "detail": {}
            })


@app.get("/mock/clearcache")
async def cache_clear(cache_manager=Depends(depends_cache_manager)):
    res = await cache_manager.clear()
    if res:
        return JSONResponse(
            status_code=200,
            content={
                "code": 100,
                "message": "success",
                "detail": {
                }
            })
    else:
        return JSONResponse(
            status_code=500,
            content={
                "code": 105,
                "message": "",
                "detail": {}
            })


@app.get("/mock/cache/{key}/{value}")
async def cache_set_key(key, value, cache_backend=Depends(depends_cache_backend)):
    # 使用backend_context完成
    res = await cache_backend.set(key, value)
    if res:
        return JSONResponse(
            status_code=200,
            content={
                "code": 100,
                "message": "success",
                "detail": {
                    "key": key,
                    "value": value
                }
            })
    else:
        return JSONResponse(
            status_code=500,
            content={
                "code": 105,
                "message": "",
                "detail": {}
            })


# ================================================================

@app.on_event("startup")
async def startup_event():
    pass


@app.on_event("shutdown")
async def shutdown_event():
    # ====================IMPORTANT WHEN USE REDIS CONNECTION POOL=========================
    await cache.destroy_backend_cache_context()
    # ======================================================================================
    pass


if __name__ == '__main__':
    uvicorn.run(app='mock_fastapi:app', host="0.0.0.0",
                port=8003, reload=True, debug=True)
