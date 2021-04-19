import logging
import os
import socket
import traceback

import ray

import pyjava.utils as utils
from pyjava.api.mlsql import RayContext
from pyjava.rayfix import RayWrapper
from pyjava.serializers import \
    write_with_length, \
    write_int, read_int, \
    SpecialLengths, ArrowStreamPandasSerializer

os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"


class SocketNotBindException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class DataServerWithId(object):
    def __init__(self, host, port, server_id):
        self.host = host
        self.port = port
        self.server_id = server_id


class OnceServer(object):
    def __init__(self, host, port, timezone):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(5 * 60)
        self.out_ser = ArrowStreamPandasSerializer(timezone, False, None)
        self.is_bind = False
        self.is_dev = utils.is_dev()

    def bind(self):
        try:
            self.socket.bind((self.host, self.port))
            self.is_bind = True
            self.socket.listen(1)
        except Exception:
            print(traceback.format_exc())

        return self.socket.getsockname()

    def close(self):
        self.socket.close()

    def serve(self, data):
        from pyjava.api.mlsql import PythonContext
        if not self.is_bind:
            raise SocketNotBindException(
                "Please invoke server.bind() before invoke server.serve")
        conn, addr = self.socket.accept()
        sockfile = conn.makefile("rwb", int(
            os.environ.get("BUFFER_SIZE", 65536)))
        infile = sockfile  # os.fdopen(os.dup(conn.fileno()), "rb", 65536)
        out = sockfile  # os.fdopen(os.dup(conn.fileno()), "wb", 65536)
        try:
            write_int(SpecialLengths.START_ARROW_STREAM, out)
            out_data = ([df[name] for name in df] for df in
                        PythonContext.build_chunk_result(data, 1024))
            self.out_ser.dump_stream(out_data, out)

            write_int(SpecialLengths.END_OF_DATA_SECTION, out)
            write_int(SpecialLengths.END_OF_STREAM, out)
            out.flush()
            if self.is_dev:
                print("all data  in ray task have been consumed.")
            read_int(infile)
        except Exception:
            try:
                write_int(SpecialLengths.ARROW_STREAM_CRASH, out)
                ex = traceback.format_exc()
                print(ex)
                write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, out)
                write_with_length(ex.encode("utf-8"), out)
                out.flush()
                read_int(infile)
            except IOError:
                # JVM close the socket
                pass
            except Exception:
                # Write the error to stderr if it happened while serializing
                print("Py worker failed with exception:")
                print(traceback.format_exc())
                pass

        conn.close()


@ray.remote
class RayDataServer(object):

    def __init__(self, server_id, java_server, port=0, timezone="Asia/Harbin"):
        self.server = OnceServer(
            RayWrapper().get_address(), port, java_server.timezone)
        try:
            (rel_host, rel_port) = self.server.bind()
        except Exception as e:
            print(traceback.format_exc())
            raise e

        self.host = rel_host
        self.port = rel_port
        self.timezone = timezone
        self.server_id = server_id
        self.java_server = java_server
        self.is_dev = utils.is_dev()

    def serve(self, func_for_row=None, func_for_rows=None):
        try:
            if func_for_row is not None:
                data = (func_for_row(item)
                        for item in RayContext.fetch_once_as_rows(self.java_server))
            elif func_for_rows is not None:
                data = func_for_rows(
                    RayContext.fetch_once_as_rows(self.java_server))
            self.server.serve(data)
        except Exception as e:
            logging.error(f"Fail to processing data in  Ray Data Server {self.host}:{self.port}")
            raise e
        finally:
            self.close()

    def close(self):
        try:
            self.server.close()
            ray.actor.exit_actor()
        except Exception:
            print(traceback.format_exc())

    def connect_info(self):
        return DataServerWithId(self.host, self.port, self.server_id)
