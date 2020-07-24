#!/usr/bin/env python
# -*- coding:utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="omi_cache_manager",
    version="0.1.3",
    keywords=("omi", "cache_manager", "asyncio", "aredis", "aio_redis"),
    description="An async cache manager implemented with [asyncio] and backends",
    long_description="An async cache manager implemented with [asyncio] and backends, \
                        supports redis, plain dictionary, null cache and etc. \
                        redis cache is implemented with aredis and aio_redis. \
                        Notes: Install aio_redis or aredis when use redis cache",
    license = "Apache License 2.0",

    url="https://github.com/limccn/omi_cache_manager",
    author = "limccn",
    author_email = "limccn@me.com",

    packages=find_packages(),
    include_package_data=True,
    platforms="any",
    install_requires=["pydantic"]
)
