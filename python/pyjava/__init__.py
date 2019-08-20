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
#

import types
from functools import wraps


def since(version):
    """
    A decorator that annotates a function to append the version of Spark the function was added.
    """
    import re
    indent_p = re.compile(r'\n( +)')

    def deco(f):
        indents = indent_p.findall(f.__doc__)
        indent = ' ' * (min(len(m) for m in indents) if indents else 0)
        f.__doc__ = f.__doc__.rstrip() + "\n\n%s.. versionadded:: %s" % (indent, version)
        return f

    return deco


def copy_func(f, name=None, sinceversion=None, doc=None):
    """
    Returns a function with same code, globals, defaults, closure, and
    name (or provide a new name).
    """
    # See
    # http://stackoverflow.com/questions/6527633/how-can-i-make-a-deepcopy-of-a-function-in-python
    fn = types.FunctionType(f.__code__, f.__globals__, name or f.__name__, f.__defaults__,
                            f.__closure__)
    # in case f was given attrs (note this dict is a shallow copy):
    fn.__dict__.update(f.__dict__)
    if doc is not None:
        fn.__doc__ = doc
    if sinceversion is not None:
        fn = since(sinceversion)(fn)
    return fn


def keyword_only(func):
    """
    A decorator that forces keyword arguments in the wrapped method
    and saves actual input keyword arguments in `_input_kwargs`.

    .. note:: Should only be used to wrap a method where first arg is `self`
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if len(args) > 0:
            raise TypeError("Method %s forces keyword arguments." % func.__name__)
        self._input_kwargs = kwargs
        return func(self, **kwargs)

    return wrapper
