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

from __future__ import print_function

import os
import sys
import time
from pyjava.utils import *

# 'resource' is a Unix specific module.
has_resource_module = True
try:
    import resource
except ImportError:
    has_resource_module = False
import traceback

from pyjava.serializers import write_with_length, write_int, read_int, SpecialLengths, UTF8Deserializer, \
    PickleSerializer

if sys.version >= '3':
    basestring = str
else:
    pass

pickleSer = PickleSerializer()
utf8_deserializer = UTF8Deserializer()


def read_command(serializer, file):
    command = serializer._read_with_length(file)
    return command


def chain(f, g):
    """chain two functions together """
    return lambda *a: g(f(*a))


def main(infile, outfile):
    try:
        boot_time = time.time()
        split_index = read_int(infile)
        if split_index == -1:  # for unit tests
            sys.exit(-1)

        version = utf8_deserializer.loads(infile)
        if version != "%d.%d" % sys.version_info[:2]:
            raise Exception(("Python in worker has different version %s than that in " +
                             "driver %s, PySpark cannot run with different minor versions." +
                             "Please check environment variables PYSPARK_PYTHON and " +
                             "PYSPARK_DRIVER_PYTHON are correctly set.") %
                            ("%d.%d" % sys.version_info[:2], version))

        # set up memory limits
        memory_limit_mb = int(os.environ.get('PYSPARK_EXECUTOR_MEMORY_MB', "-1"))
        if memory_limit_mb > 0 and has_resource_module:
            total_memory = resource.RLIMIT_AS
            try:
                (soft_limit, hard_limit) = resource.getrlimit(total_memory)
                msg = "Current mem limits: {0} of max {1}\n".format(soft_limit, hard_limit)
                print(msg, file=sys.stderr)

                # convert to bytes
                new_limit = memory_limit_mb * 1024 * 1024

                if soft_limit == resource.RLIM_INFINITY or new_limit < soft_limit:
                    msg = "Setting mem limits to {0} of max {1}\n".format(new_limit, new_limit)
                    print(msg, file=sys.stderr)
                    resource.setrlimit(total_memory, (new_limit, new_limit))

            except (resource.error, OSError, ValueError) as e:
                # not all systems support resource limits, so warn instead of failing
                print("WARN: Failed to set memory limit: {0}\n".format(e), file=sys.stderr)

        func, profiler, deserializer, serializer = read_command(pickleSer, infile)

        def process():
            iterator = deserializer.load_stream(infile)
            out_iter = func(split_index, iterator)
            try:
                serializer.dump_stream(out_iter, outfile)
            finally:
                if hasattr(out_iter, 'close'):
                    out_iter.close()

        if profiler:
            profiler.profile(process)
        else:
            process()
    except Exception:
        try:
            write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, outfile)
            write_with_length(traceback.format_exc().encode("utf-8"), outfile)
        except IOError:
            # JVM close the socket
            pass
        except Exception:
            # Write the error to stderr if it happened while serializing
            print("PySpark worker failed with exception:", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
        sys.exit(-1)

    if read_int(infile) == SpecialLengths.END_OF_STREAM:
        write_int(SpecialLengths.END_OF_STREAM, outfile)
    else:
        # write a different value to tell JVM to not reuse this worker
        write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
        sys.exit(-1)


if __name__ == '__main__':
    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
    main(sock_file, sock_file)
