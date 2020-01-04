import os
import socket
import traceback

import ray

from pyjava.serializers import \
    write_with_length, \
    write_int, read_int, \
    SpecialLengths, ArrowStreamPandasSerializer


class SocketNotBindException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class OnceServer(object):
    def __init__(self, host, port, timezone):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(5 * 60)
        self.out_ser = ArrowStreamPandasSerializer(timezone, False, None)
        self.is_bind = False

    def bind(self):
        try:
            self.socket.bind((self.host, self.port))
            self.is_bind = True
        except socket.error as msg:
            print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])

        self.socket.listen(1)
        return self.socket.getsockname()

    def serve(self, data):
        if not self.is_bind:
            raise SocketNotBindException("Please invoke server.bind() before invoke server.serve")
        conn, addr = self.socket.accept()
        sockfile = conn.makefile("rwb", int(os.environ.get("BUFFER_SIZE", 65536)))
        infile = sockfile  # os.fdopen(os.dup(conn.fileno()), "rb", 65536)
        out = sockfile  # os.fdopen(os.dup(conn.fileno()), "wb", 65536)
        try:
            write_int(SpecialLengths.START_ARROW_STREAM, out)
            self.out_ser.dump_stream(data, out)
            write_int(SpecialLengths.END_OF_DATA_SECTION, out)
            write_int(SpecialLengths.END_OF_STREAM, out)
            out.flush()
            read_int(infile)
        except Exception:
            try:
                write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, out)
                write_with_length(traceback.format_exc().encode("utf-8"), out)
            except IOError:
                # JVM close the socket
                pass
            except Exception:
                # Write the error to stderr if it happened while serializing
                print("Py worker failed with exception:")
                print(traceback.format_exc())
                pass

        write_int(SpecialLengths.END_OF_STREAM, out)
        conn.close()


@ray.remote
class RayDataServer(object):

    def __init__(self, port, timezone="Asia/Harbin"):
        os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"
        self.server = OnceServer(self.get_address(), self.port, self.timezone)
        (rel_host, rel_port) = self.server.bind()
        self.host = rel_host
        self.port = rel_port
        self.timezone = timezone

    def serve(self, data):
        self.server.serve(data)

    def get_address(self):
        return ray.services.get_node_ip_address()
