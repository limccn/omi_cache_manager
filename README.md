## omi_cache_manager

### Description 
An cache manager implement with `asyncio` backends

### Usage
1.Install omi_cache_manager from `pip`

```shell script
$pip install omi_cache_manager
```
or install from source code
```shell script
$python setup.py install
```
 
2.Install redis backend for omi_cache_manager, [aioredis](https://github.com/aio-libs/aioredis/) or [aredis](https://github.com/NoneGG/aredis)

```shell script
 # use aioredis as backend
 $pip install aioredis
```

```shell script
 # use aredis as backend
 $pip install aredis
```  

Backend support list

| Backend Name | Type | Module | Class | Alias |
|-|-|-|-|
|null | Simulate | omi_cache_manager.backends | NullCacheBackend | null_cache |
|simple map | Memory | omi_cache_manager.backends | SimpleCacheBackend | simple_cache |
|[aioredis](https://github.com/aio-libs/aioredis/) | Async/Sync | omi_cache_manager.aio_redis_backend | AIORedisBackend | aioredis |
|[aredis](https://github.com/NoneGG/aredis) | Async/Sync | omi_cache_manager.aredis_backend | ARedisBackend | aredis |

3.Apply to your project.

```python
# use redis server as cache context manager
cache = AsyncCacheManager(
    app, # None if no app context to set
    # use backend alias ARedisBackend, or aredis 
    cache_backend="ARedisBackend", # will convert to omi_cache_manager.aredis_backend.ARedisBackend
    config={
        "CACHE_REDIS_SCHEME": "redis",
        "CACHE_REDIS_HOST": "localhost",
        "CACHE_REDIS_PORT": 6379,
        "CACHE_REDIS_PASSWORD": "",
        "CACHE_REDIS_DATABASE": 0,
    }
)
```
```python
# use simple dictionary as cache context manager
cache = AsyncCacheManager(
    app, # None if no app context to set
    # use backend alias SimpleCacheBackend, or simple_cache
    cache_backend="SimpleCacheBackend", # will convert to omi_cache_manager.backends.SimpleCacheBackend
    config={
        "CACHE_KEY_PREFIX": "MOCK_SIMPLE_INTEGRATION_TEST:"
    }
)
```

4.Test Cache if is work, and enjoy omi_cache_manager
```python
# GET
value = await cache.get("key")
# GET MANY
value = await cache.get_many("key1", "key2", "key3")
# SET
value = await cache.set("key", "val")
value = await cache.set(key="key", value="val")
# SET MANY, tuple, mapping is supported
value = await cache.set_many(key1="val1", key2="val2")
value = await cache.set_many(("key1", "val1"), ("key2", "val2"))
# ADD
value = await cache.add("key", "val")
value = await cache.add(key="key", value="val")
# DELETE
value = await cache.delete("key")
# DELETE MANY
value = await cache.delete_many("key1","key2","key3")

```

5.Close cache connection or destroy cache stored in memory
```python
# async model
await cache.destroy_backend_cache_context()
# sync model
cache.destroy_backend_cache_context()
```

5.We implemented a demo api provider use [FastAPI](https://github.com/tiangolo/fastapi) to show How to use this library. and testing is included.

@See [mock_fastapi.py](https://github.com/limccn/omi_cache_manager/blob/master/mock_fastapi.py) for detail

### License

##### omi_cache_manager is released under the Apache License 2.0.

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