#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import sys

from setuptools import setup

if sys.version_info < (3, 6):
    print("Python versions prior to 2.7 are not supported for pip installed pyjava.",
          file=sys.stderr)
    sys.exit(-1)

try:
    exec(open('pyjava/version.py').read())
except IOError:
    print("Failed to load PyJava version file for packaging. You must be in PyJava's python dir.",
          file=sys.stderr)
    sys.exit(-1)
VERSION = __version__  # noqa
# A temporary path so we can access above the Python project root and fetch scripts and jars we need
TEMP_PATH = "deps"

# Provide guidance about how to use setup.py
incorrect_invocation_message = """
If you are installing pyjava from source, you must first build 
run sdist.
    Building the source dist is done in the Python directory:
      cd python
      python setup.py sdist
      pip install dist/*.tar.gz"""

_minimum_pandas_version = "0.23.2"
_minimum_pyarrow_version = "0.12.1"

try:
    setup(
        name='pyjava',
        version=VERSION,
        description='PyJava Python API',
        long_description="",
        author='allwefantasy',
        author_email='allwefantasy@gmail.com',
        url='https://github.com/allwefantasy/pyjava',
        packages=['pyjava',
                  'pyjava.api',
                  'pyjava.udf',
                  'pyjava.datatype',
                  'pyjava.storage',
                  'pyjava.cache'],
        include_package_data=True,
        package_dir={
            'pyjava.sbin': 'deps/sbin'
        },
        package_data={
            'pyjava.sbin': []},
        license='http://www.apache.org/licenses/LICENSE-2.0',
        install_requires=['py4j==0.10.8.1'],
        setup_requires=['pypandoc'],
        extras_require={
            'pyjava': [
                'pandas>=%s' % _minimum_pandas_version,
                'pyarrow>=%s' % _minimum_pyarrow_version,
            ]
        },
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: Implementation :: CPython',
            'Programming Language :: Python :: Implementation :: PyPy']
    )
finally:
    print("--------")
