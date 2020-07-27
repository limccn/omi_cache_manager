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
import time

import pytest
from fastapi.testclient import TestClient

sys.path.append("../")

from mock_fastapi import app

test_client = TestClient(app)


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


def test_clear(setup_module):
    resp = test_client.get(url="/mock/clearcache")
    assert resp.status_code == 200
    time.sleep(0.1)  # wait to clear


def test_get_key(setup_module):
    resp = test_client.get(url="/mock/cache/foo")
    assert resp.status_code == 404


def test_set_key(setup_module):
    resp = test_client.get(url="/mock/cache/foo/bar")
    assert resp.status_code == 200


def test_get_set_clear(setup_module):
    # clear 
    resp = test_client.get(url="/mock/clearcache?rnd=2")
    assert resp.status_code == 200
    time.sleep(0.1)  # wait to clear
    # get for none
    resp = test_client.get(url="/mock/cache/foo?rnd=1")
    assert resp.status_code == 404
    # set value
    resp = test_client.get(url="/mock/cache/foo/bar?rnd=2")
    assert resp.status_code == 200
    # get value
    resp = test_client.get(url="/mock/cache/foo?rnd=2")
    assert resp.status_code == 200
    # clear 
    resp = test_client.get(url="/mock/clearcache?rnd=3")
    assert resp.status_code == 200
    time.sleep(0.1)  # wait to clear
    # get for none
    resp = test_client.get(url="/mock/cache/foo?rnd=3")
    assert resp.status_code == 404


if __name__ == '__main__':
    pytest.main(['test_integration_omi_cache_manger.py'])
