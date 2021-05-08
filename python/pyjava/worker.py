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

from pyjava.api.mlsql import PythonContext
from pyjava.cache.code_cache import CodeCache
from pyjava.utils import *

# 'resource' is a Unix specific module.
has_resource_module = True
try:
    import resource
except ImportError:
    has_resource_module = False

import traceback
import logging

from pyjava.serializers import \
    write_with_length, \
    write_int, \
    read_int, read_bool, SpecialLengths, UTF8Deserializer, \
    PickleSerializer, ArrowStreamPandasSerializer, ArrowStreamSerializer

if sys.version >= '3':
    basestring = str
else:
    pass

# logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level='DEBUG')

pickleSer = PickleSerializer()
utf8_deserializer = UTF8Deserializer()

globals_namespace = globals()


def read_command(serializer, file):
    command = serializer.load_stream(file)
    return command


def chain(f, g):
    """chain two functions together """
    return lambda *a: g(f(*a))


def main(infile, outfile):
    try:
        try:
            import ray
        except ImportError:
            pass
        # set up memory limits
        memory_limit_mb = int(os.environ.get('PY_EXECUTOR_MEMORY', "-1"))
        if memory_limit_mb > 0 and has_resource_module:
            total_memory = resource.RLIMIT_AS
            try:
                (soft_limit, hard_limit) = resource.getrlimit(total_memory)
                msg = "Current mem limits: {0} of max {1}\n".format(soft_limit, hard_limit)
                logging.info(msg)

                # convert to bytes
                new_limit = memory_limit_mb * 1024 * 1024

                if soft_limit == resource.RLIM_INFINITY or new_limit < soft_limit:
                    msg = "Setting mem limits to {0} of max {1}\n".format(new_limit, new_limit)
                    logging.info(msg)
                    resource.setrlimit(total_memory, (new_limit, new_limit))

            except (resource.error, OSError, ValueError) as e:
                # not all systems support resource limits, so warn instead of failing
                logging.warning("WARN: Failed to set memory limit: {0}\n".format(e))
        split_index = read_int(infile)
        logging.info("split_index:%s" % split_index)
        if split_index == -1:  # for unit tests
            sys.exit(-1)

        is_barrier = read_bool(infile)
        bound_port = read_int(infile)
        logging.info(f"is_barrier {is_barrier}, port {bound_port}")

        conf = {}
        for i in range(read_int(infile)):
            k = utf8_deserializer.loads(infile)
            v = utf8_deserializer.loads(infile)
            conf[k] = v
            logging.debug(f"conf {k}:{v}")

        command = utf8_deserializer.loads(infile)
        ser = ArrowStreamSerializer()

        timezone = conf["timezone"] if "timezone" in conf else None

        out_ser = ArrowStreamPandasSerializer(timezone, True, True)
        is_interactive = os.environ.get('PY_INTERACTIVE', "no") == "yes"
        import uuid
        context_id = str(uuid.uuid4())

        if not os.path.exists(context_id):
            os.mkdir(context_id)

        def process():
            try:
                input_data = ser.load_stream(infile)
                code = CodeCache.get(command)
                if is_interactive:
                    global data_manager
                    global context
                    data_manager = PythonContext(context_id, input_data, conf)
                    context = data_manager
                    global globals_namespace
                    exec(code, globals_namespace, globals_namespace)
                else:
                    data_manager = PythonContext(context_id, input_data, conf)
                    n_local = {"data_manager": data_manager, "context": data_manager}
                    exec(code, n_local, n_local)
                out_iter = data_manager.output()
                write_int(SpecialLengths.START_ARROW_STREAM, outfile)
                out_ser.dump_stream(out_iter, outfile)
            finally:

                try:
                    import shutil
                    shutil.rmtree(context_id)
                except:
                    pass

                try:
                    if hasattr(out_iter, 'close'):
                        out_iter.close()
                except:
                    pass

                try:
                    del data_manager
                except:
                    pass

        process()


    except Exception:
        try:
            write_int(SpecialLengths.ARROW_STREAM_CRASH, outfile)
            write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, outfile)
            write_with_length(traceback.format_exc().encode("utf-8"), outfile)
        except IOError:
            # JVM close the socket
            pass
        except Exception:
            # Write the error to stderr if it happened while serializing
            print("Py worker failed with exception:", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
        sys.exit(-1)

    write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
    flag = read_int(infile)
    if flag == SpecialLengths.END_OF_STREAM:
        write_int(SpecialLengths.END_OF_STREAM, outfile)
    else:
        # write a different value to tell JVM to not reuse this worker
        write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
        sys.exit(-1)


if __name__ == '__main__':
    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    (sock_file, _) = local_connect_and_auth(java_port)
    main(sock_file, sock_file)
