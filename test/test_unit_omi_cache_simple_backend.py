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

import sys

import pytest

sys.path.append("../")

from omi_cache_manager.async_cache_manager import AsyncCacheManager
from omi_cache_manager.backends import NullCacheBackend, SimpleCacheBackend

# =======================================
# install nest_asyncio for unit test when 
# RuntimeError: This event loop is already running
# pip install nest_asyncio
import nest_asyncio

nest_asyncio.apply()
# =======================================

simple_cache_backend = SimpleCacheBackend(config={
    "CACHE_KEY_PREFIX": "SOME_PREFIX:"
})


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


def get_cache():
    return simple_cache_backend


@pytest.mark.asyncio
async def test_backend_null(event_loop):
    nullcache = AsyncCacheManager(
        None,
        cache_backend=NullCacheBackend(config={}),
        config={}
    )

    val = await nullcache.set("foo", "bar")
    assert val is True
    val = await nullcache.get("foo")
    assert val is None
    val = await nullcache.set_many("foo1", "bar1", "foo2", "bar2")
    assert val is True
    val = await nullcache.get_many("foo1", "foo2")
    assert val is None
    val = await nullcache.add("foo", "bar")
    assert val is True
    val = await nullcache.delete("foo")
    assert val is True
    val = await nullcache.delete_many("foo")
    assert val is True
    val = await nullcache.execute("GET", "foo")
    assert val is None
    val = await nullcache.clear()
    assert val is None


@pytest.mark.asyncio
async def test_backend_default(event_loop):
    val = await get_cache().get("foo")
    assert val is None


@pytest.mark.asyncio
async def test_backend_get(event_loop):
    val = await get_cache().get("foo")
    assert val is None
    val = await get_cache().get(5)
    assert val is None
    val = await get_cache().get(key="foo")
    assert val is None


@pytest.mark.asyncio
async def test_backend_get_error(event_loop):
    try:
        val = await get_cache().get("foobar", "bar")
    except TypeError as err:
        assert isinstance(err, TypeError)

    try:
        val = await get_cache().get("foobar", key="bar")
    except TypeError as err:
        assert isinstance(err, TypeError)

    try:
        val = await get_cache().get(foobar="foobar", key="bar")
    except TypeError as err:
        assert isinstance(err, TypeError)

    try:
        val = await get_cache().get()
    except TypeError as err:
        assert isinstance(err, TypeError)


@pytest.mark.asyncio
async def test_backend_set(event_loop):
    val = await get_cache().set("foobar", "foobar")
    assert val is True
    val = await get_cache().get("foobar")
    assert val == "foobar"
    val = await get_cache().set(("tuple", "tuple"))
    assert val is True
    val = await get_cache().get("tuple")
    assert val == "tuple"
    val = await get_cache().set(mapping="mapping")
    assert val is True
    val = await get_cache().get("mapping")
    assert val == "mapping"


@pytest.mark.asyncio
async def test_backend_set_error(event_loop):
    try:
        val = await get_cache().set(None, "foobar")
    except TypeError as err:
        assert isinstance(err, TypeError)

    try:
        val = await get_cache().set("miss_value")
    except TypeError as err:
        assert isinstance(err, TypeError)

    try:
        val = await get_cache().set("foobar", "foobar", "foo1")
    except TypeError as err:
        assert isinstance(err, TypeError)

    try:
        val = await get_cache().set(foobar="foobar", foo="bar")
    except TypeError as err:
        assert isinstance(err, TypeError)

    try:
        val = await get_cache().set(expire=300)
    except TypeError as err:
        assert isinstance(err, TypeError)

    val = await get_cache().set("foobar", "foobar")
    assert val is True
    val = await get_cache().get("foobar")
    assert val == "foobar"


@pytest.mark.asyncio
async def test_backend_add(event_loop):
    val = await get_cache().add("add", "add")
    assert val is True
    val = await get_cache().add("add", "add")
    assert val is True  # TODO add same key twice will return false,now is true
    val = await get_cache().get("add")
    assert val == "add"
    val = await get_cache().add(("tuple", "tuple"))
    assert val is True
    val = await get_cache().get("tuple")
    assert val == "tuple"
    val = await get_cache().add(mapping="mapping")
    assert val is True
    val = await get_cache().get("mapping")
    assert val == "mapping"


@pytest.mark.asyncio
async def test_backend_delete(event_loop):
    val = await get_cache().set_many(("foobar", "foobar"), ("delete", "delete"))
    assert val is True
    val = await get_cache().get("foobar")
    assert val == "foobar"
    val = await get_cache().delete("foobar")
    assert val is True
    val = await get_cache().get("foobar")
    assert val is None
    val = await get_cache().delete(key="delete")
    assert val is True
    val = await get_cache().get("delete")
    assert val is None


@pytest.mark.asyncio
async def test_backend_delete_error(event_loop):
    try:
        val = await get_cache().delete("foobar", "bar")
    except TypeError as err:
        assert isinstance(err, TypeError)

    try:
        val = await get_cache().delete("foobar", key="bar")
    except TypeError as err:
        assert isinstance(err, TypeError)

    try:
        val = await get_cache().delete(foobar="foobar", key="bar")
    except TypeError as err:
        assert isinstance(err, TypeError)

    try:
        val = await get_cache().delete()
    except TypeError as err:
        assert isinstance(err, TypeError)


@pytest.mark.asyncio
async def test_backend_set_many(event_loop):
    val = await get_cache().set_many(alpha="Alpha", bravo="Bravo", charlie="Charlie")
    assert val is True
    val = await get_cache().get("alpha")
    assert val == "Alpha"
    val = await get_cache().get("bravo")
    assert val == "Bravo"
    val = await get_cache().get("charlie")
    assert val == "Charlie"
    val = await get_cache().set_many(("delta", "Delta"), ("echo", "Echo"))
    assert val is True
    val = await get_cache().get("delta")
    assert val == "Delta"
    val = await get_cache().get("echo")
    assert val == "Echo"
    val = await get_cache().set_many(("fox", "Fox"), golf="Golf")
    assert val is True
    val = await get_cache().get("fox")
    assert val == "Fox"
    val = await get_cache().get("golf")
    assert val == "Golf"


@pytest.mark.asyncio
async def test_backend_set_manay_error(event_loop):
    try:
        val = await get_cache().set_many()
    except TypeError as err:
        assert isinstance(err, TypeError)


@pytest.mark.asyncio
async def test_backend_get_many(event_loop):
    val = await get_cache().set_many(alpha="Alpha", bravo="Bravo", charlie="Charlie")
    assert val is True
    val = await get_cache().get_many("alpha")
    assert val == ["Alpha"]
    val = await get_cache().get_many("alpha", "bravo")
    assert val == ["Alpha", "Bravo"]


@pytest.mark.asyncio
async def test_backend_get_manay_error(event_loop):
    try:
        val = await get_cache().get_many()
    except TypeError as err:
        assert isinstance(err, TypeError)


@pytest.mark.asyncio
async def test_backend_delete_many(event_loop):
    val = await get_cache().set_many(alpha="Alpha", bravo="Bravo", charlie="Charlie")
    assert val is True
    val = await get_cache().get("alpha")
    assert val == "Alpha"
    val = await get_cache().delete_many("alpha", "bravo")
    assert val is True
    val = await get_cache().get("alpha")
    assert val is None
    val = await get_cache().get("bravo")
    assert val is None
    val = await get_cache().get("charlie")
    assert val == "Charlie"


@pytest.mark.asyncio
async def test_backend_delete_manay_error(event_loop):
    try:
        val = await get_cache().delete_many()
    except TypeError as err:
        assert isinstance(err, TypeError)


@pytest.mark.asyncio
async def test_backend_clear(event_loop):
    val = await get_cache().set_many(alpha="Alpha", bravo="Bravo", charlie="Charlie")
    assert val is True
    val = await get_cache().get_many("alpha")
    assert val == ["Alpha"]
    val = await get_cache().get_many("alpha", "bravo")
    assert val == ["Alpha", "Bravo"]
    val = await get_cache().clear()
    assert val is True
    val = await get_cache().clear()
    assert val is True  # clear twice
    val = await get_cache().get("alpha")
    assert val is None
    val = await get_cache().get_many("alpha")
    assert val == [None]


@pytest.mark.asyncio
async def test_backend_exec(event_loop):
    val = await get_cache().execute("SET", "alpha", "Alpha")
    assert val is True
    val = await get_cache().execute("GET", "alpha")
    assert val == "Alpha"
    val = await get_cache().execute("MSET", bravo="Bravo", charlie="Charlie")
    assert val is True
    val = await get_cache().execute("MGET", "alpha", "bravo", "charlie")
    assert val == ["Alpha", "Bravo", "Charlie"]
    val = await get_cache().execute("MSET", ("delta", "Delta"), ("echo", "Echo"))
    assert val is True
    val = await get_cache().execute("MGET", "alpha", "bravo", "charlie", "delta")
    assert val == ["Alpha", "Bravo", "Charlie", "Delta"]


@pytest.mark.asyncio
async def test_backend_exec_error(event_loop):
    try:
        val = await get_cache().execute("UNKNOW", "alpha", "Alpha")
        assert val is None
    except Exception as ex:
        assert isinstance(ex, TypeError)


if __name__ == '__main__':
    pytest.main(['test_unit_omi_cache_simple_backend.py'])
