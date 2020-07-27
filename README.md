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
 
2.Install backend for omi_cache_manager, [aioredis](https://github.com/aio-libs/aioredis/) or [aredis](https://github.com/NoneGG/aredis)

```python
 # use aioredis as backend
 $pip install aioredis
```

```python
 # use aredis as backend
 $pip install aredis
```  
3.Apply to your project.

```python
# use redis server as cache context manager
cache = AsyncCacheManager(
    app, # None if no app context to set
    cache_backend="omi_cache_manager.aredis_backend.ARedisBackend",
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
    cache_backend="omi_cache_manager.backends.SimpleCacheBackend",
    config={
        "CACHE_KEY_PREFIX": "MOCK_SIMPLE_INTEGRATION_TEST:"
    }
)
```

4.Test Cache if is work, and enjoy omi_cache_manager
```python
# GET
value = await cache.get("key")
# SET
value = await cache.set("key", "val")
# ADD
value = await cache.add("key", "val")
# DELETE
value = await cache.delete("key")

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