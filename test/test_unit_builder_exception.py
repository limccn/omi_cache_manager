import os

import pytest

from omi_cache_manager.aio_redis_backend import AIORedisBackend, AIORedisContext
from omi_cache_manager.aredis_backend import ARedisBackend, ARedisContext
from omi_cache_manager.async_cache_manager import AsyncCacheManager
from omi_cache_manager.backends import NullCacheBackend
from omi_cache_manager.backends import SimpleCacheBackend, SimpleCacheDictContext


@pytest.fixture(scope='function')
def setup_function(request):
    def teardown_function():
        print("teardown_function called.")

    request.addfinalizer(teardown_function)
    print('setup_function called.')


@pytest.fixture(scope='module')
def setup_module(request):
    def teardown_module():
        print("teardown_module called.")

    request.addfinalizer(teardown_module)
    print('setup_module called.')


def test_builder_error(setup_module):
    try:
        cache_manger = AsyncCacheManager(
            None,
            cache_backend="null_cache",
            config=tuple(["key", "val"]),
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)
    try:
        cache_manger = AsyncCacheManager(
            None,
            cache_backend="simple_cache",
            config=tuple(["key", "val"]),
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)
    try:
        cache_manger = AsyncCacheManager(
            None,
            cache_backend="aioredis",
            config=tuple(["key", "val"]),
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)
    try:
        cache_manger = AsyncCacheManager(
            None,
            cache_backend="aredis",
            config=tuple(["key", "val"]),
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)


def test_backend_builder_error(setup_module):
    try:
        cache_manger = NullCacheBackend(
            config=tuple(["key", "val"]),
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)
    try:
        cache_backend = SimpleCacheBackend(
            config=tuple(["key", "val"]),
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)
    try:
        cache_manger = AIORedisBackend(
            config=tuple(["key", "val"]),
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)
    try:
        cache_manger = ARedisBackend(
            config=tuple(["key", "val"]),
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)


def test_builder_aioredis(setup_module):
    cache_manger = AsyncCacheManager(
        None,
        cache_backend="omi_cache_manager.aio_redis_backend.AIORedisBackend",
        config={
            "CACHE_KEY_PREFIX": "AIO_REDIS_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, AIORedisBackend)

    cache_manger = AsyncCacheManager(
        None,
        cache_backend="aioredis",
        config={
            "CACHE_KEY_PREFIX": "AIO_REDIS_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, AIORedisBackend)

    cache_manger = AsyncCacheManager(
        None,
        cache_backend="AIORedisBackend",
        config={
            "CACHE_KEY_PREFIX": "AIO_REDIS_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, AIORedisBackend)


def test_builder_aredis(setup_module):
    cache_manger = AsyncCacheManager(
        None,
        cache_backend="omi_cache_manager.aredis_backend.ARedisBackend",
        config={
            "CACHE_KEY_PREFIX": "AREDIS_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, ARedisBackend)

    cache_manger = AsyncCacheManager(
        None,
        cache_backend="aredis",
        config={
            "CACHE_KEY_PREFIX": "AREDIS_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, ARedisBackend)

    cache_manger = AsyncCacheManager(
        None,
        cache_backend="ARedisBackend",
        config={
            "CACHE_KEY_PREFIX": "AREDIS_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, ARedisBackend)


def test_builder_simple(setup_module):
    cache_manger = AsyncCacheManager(
        None,
        cache_backend="omi_cache_manager.backends.SimpleCacheBackend",
        config={
            "CACHE_KEY_PREFIX": "SIMPLE_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, SimpleCacheBackend)

    cache_manger = AsyncCacheManager(
        None,
        cache_backend="simple_cache",
        config={
            "CACHE_KEY_PREFIX": "SIMPLE_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, SimpleCacheBackend)

    cache_manger = AsyncCacheManager(
        None,
        cache_backend="SimpleCacheBackend",
        config={
            "CACHE_KEY_PREFIX": "SIMPLE_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, SimpleCacheBackend)


def test_builder_null(setup_module):
    cache_manger = AsyncCacheManager(
        None,
        cache_backend="omi_cache_manager.backends.NullCacheBackend",
        config={
            "CACHE_KEY_PREFIX": "NULL_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, NullCacheBackend)

    cache_manger = AsyncCacheManager(
        None,
        cache_backend="null_cache",
        config={
            "CACHE_KEY_PREFIX": "NULL_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, NullCacheBackend)

    cache_manger = AsyncCacheManager(
        None,
        cache_backend="NullCacheBackend",
        config={
            "CACHE_KEY_PREFIX": "NULL_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, NullCacheBackend)


def test_null_backend_with_config(event_loop):
    # None is Ok
    test_backend_with_config = NullCacheBackend(
        config=None
    )
    assert test_backend_with_config.key_prefix == test_backend_with_config.__class__.__name__.upper()

    test_backend_with_config = NullCacheBackend(
        config={}
    )
    assert test_backend_with_config.key_prefix == test_backend_with_config.__class__.__name__.upper()

    try:
        test_backend_with_config = NullCacheBackend(
            config=tuple(["config", "value"])
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)


def test_simple_backend_with_config(event_loop):
    # None is Ok
    test_backend_with_config = SimpleCacheBackend(
        config=None
    )
    assert test_backend_with_config.key_prefix == test_backend_with_config.__class__.__name__.upper()

    # empty dict
    test_backend_with_config = SimpleCacheBackend(
        config={}
    )
    assert test_backend_with_config.key_prefix == test_backend_with_config.__class__.__name__.upper()

    try:
        test_backend_with_config = SimpleCacheBackend(
            config=tuple(["config", "value"])
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)


def test_aio_redis_backend_with_config(event_loop):
    # None is ok
    test_backend_with_config = AIORedisBackend(
        config=None
    )
    assert test_backend_with_config.redis_host == "localhost"
    assert test_backend_with_config.redis_port == 6379
    assert test_backend_with_config.key_prefix == test_backend_with_config.__class__.__name__.upper()

    # empty dict
    test_backend_with_config = AIORedisBackend(
        config={}
    )
    assert test_backend_with_config.redis_host == "localhost"
    assert test_backend_with_config.redis_port == 6379
    assert test_backend_with_config.key_prefix == test_backend_with_config.__class__.__name__.upper()

    try:
        test_backend_with_config = AIORedisBackend(
            config=tuple(["config", "value"])
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)


def test_aredis_backend_with_config(event_loop):
    # None is ok, use default
    test_backend_with_config = ARedisBackend(
        config=None
    )
    assert test_backend_with_config.redis_host == "localhost"
    assert test_backend_with_config.redis_port == 6379
    assert test_backend_with_config.key_prefix == test_backend_with_config.__class__.__name__.upper()

    # empty dict
    test_backend_with_config = ARedisBackend(
        config={}
    )
    assert test_backend_with_config.redis_host == "localhost"
    assert test_backend_with_config.redis_port == 6379
    assert test_backend_with_config.key_prefix == test_backend_with_config.__class__.__name__.upper()

    try:
        test_backend_with_config = ARedisBackend(
            config=tuple(["config", "value"])
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)


class MockContext:
    pass


@pytest.mark.asyncio
async def test_builder_bind_app(event_loop):
    test_app = MockContext()
    setattr(test_app, "state", None)
    cache_manger = AsyncCacheManager(
        test_app,
        cache_backend="omi_cache_manager.aio_redis_backend.AIORedisBackend",
        config={
            "CACHE_KEY_PREFIX": "AIO_REDIS_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.app_ref, MockContext)

    test_app = MockContext()
    setattr(test_app, "state", MockContext())
    cache_manger = AsyncCacheManager(
        test_app,
        cache_backend="omi_cache_manager.aio_redis_backend.AIORedisBackend",
        config={
            "CACHE_KEY_PREFIX": "AIO_REDIS_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.app_ref, MockContext)
    assert isinstance(cache_manger.app_ref.state, MockContext)
    assert isinstance(cache_manger.app_ref.state.OMI_CACHE_MANAGER, AsyncCacheManager)

    # bind twice cause error
    try:
        cache_manger = AsyncCacheManager(
            test_app,
            cache_backend="omi_cache_manager.aio_redis_backend.AIORedisBackend",
            config={
                "CACHE_KEY_PREFIX": "AIO_REDIS_MANAGER_UNIT_TEST_POOL:"
            }
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)

    test_app = MockContext()
    test_app.state = MockContext()
    test_app.state.OMI_CACHE_MANAGER = MockContext()

    # bind before set to AsyncCacheManager
    try:
        cache_manger = AsyncCacheManager(
            test_app,
            cache_backend="omi_cache_manager.aio_redis_backend.AIORedisBackend",
            config={
                "CACHE_KEY_PREFIX": "AIO_REDIS_MANAGER_UNIT_TEST_POOL:"
            }
        )
    except Exception as ex:
        assert isinstance(ex, ValueError)


@pytest.mark.asyncio
async def test_builder_aredis_create_destory(event_loop):
    cache_manger = AsyncCacheManager(
        None,
        cache_backend="omi_cache_manager.aio_redis_backend.AIORedisBackend",
        config={
            "CACHE_KEY_PREFIX": "AIO_REDIS_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, AIORedisBackend)

    await cache_manger.destroy_backend_cache_context()

    cache_context = cache_manger.handle_backend_cache_context()
    assert cache_context is None


@pytest.mark.asyncio
async def test_builder_aredis_create_destory(event_loop):
    cache_manger = AsyncCacheManager(
        None,
        cache_backend="ARedisBackend",
        config={
            "CACHE_REDIS_SCHEME": "redis",
            "CACHE_REDIS_HOST": "192.168.201.169",
            "CACHE_REDIS_PORT": 6379,
            "CACHE_REDIS_USER": "user",
            "CACHE_REDIS_PASSWORD": "",
            "CACHE_REDIS_DATABASE": 8,

            'CACHE_REDIS_CONNECTION_TIMEOUT': 3,
            'CACHE_REDIS_ENCODING': 'utf-8',

            'CACHE_REDIS_USE_CLUSTER': False,  # for cluster not tested
            'CACHE_REDIS_USE_POOL': False,
            'CACHE_REDIS_POOL_MINSIZE': 1,  # no effect
            'CACHE_REDIS_POOL_MAXSIZE': 50,

            "CACHE_KEY_PREFIX": "A_REDIS_MANAGER_UNIT_TEST:"
        }
    )
    assert isinstance(cache_manger.cache_backend, ARedisBackend)

    val = await cache_manger.get("foo")
    assert val is None

    cache_context = cache_manger.handle_backend_cache_context()
    assert isinstance(cache_context, ARedisContext)

    await cache_manger.destroy_backend_cache_context()

    cache_context = cache_manger.handle_backend_cache_context()
    assert cache_context is None


@pytest.mark.asyncio
async def test_builder_aioredis_create_destory(event_loop):
    cache_manger = AsyncCacheManager(
        None,
        cache_backend="AIORedisBackend",
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
    assert isinstance(cache_manger.cache_backend, AIORedisBackend)

    val = await cache_manger.get("foo")
    assert val is None

    cache_context = cache_manger.handle_backend_cache_context()
    assert isinstance(cache_context, AIORedisContext)

    await cache_manger.destroy_backend_cache_context()


@pytest.mark.asyncio
async def test_builder_simple_create_destory(event_loop):
    cache_manger = AsyncCacheManager(
        None,
        cache_backend="simple_cache",
        config={
            "CACHE_KEY_PREFIX": "SIMPLE_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, SimpleCacheBackend)

    val = await cache_manger.get("foo")
    assert val is None

    cache_context = cache_manger.handle_backend_cache_context()
    assert isinstance(cache_context, SimpleCacheDictContext)

    await cache_manger.destroy_backend_cache_context()


@pytest.mark.asyncio
async def test_builder_null_create_destory(event_loop):
    cache_manger = AsyncCacheManager(
        None,
        cache_backend="null_cache",
        config={
            "CACHE_KEY_PREFIX": "NULL_MANAGER_UNIT_TEST_POOL:"
        }
    )
    assert isinstance(cache_manger.cache_backend, NullCacheBackend)

    val = await cache_manger.get("foo")
    assert val is None

    cache_context = cache_manger.handle_backend_cache_context()
    assert cache_context is None

    create_result = cache_manger.create_backend_cache_context()
    assert create_result is None

    cache_context = cache_manger.handle_backend_cache_context()
    assert cache_context is None

    await cache_manger.destroy_backend_cache_context()


if __name__ == '__main__':
    pytest.main([os.path.basename(__file__)])
