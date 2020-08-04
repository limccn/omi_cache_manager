import os

import pytest

from omi_cache_manager.aio_redis_backend import AIORedisBackend
from omi_cache_manager.aredis_backend import ARedisBackend
from omi_cache_manager.async_cache_manager import AsyncCacheManager
from omi_cache_manager.backends import NullCacheBackend
from omi_cache_manager.backends import SimpleCacheBackend


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


if __name__ == '__main__':
    pytest.main([os.path.basename(__file__)])
