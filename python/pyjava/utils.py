import os
import sys

from pyjava.serializers import write_with_length, UTF8Deserializer, \
    PickleSerializer

if sys.version >= '3':
    basestring = str
else:
    pass

pickleSer = PickleSerializer()
utf8_deserializer = UTF8Deserializer()


def require_minimum_pandas_version():
    """ Raise ImportError if minimum version of Pandas is not installed
    """
    # TODO(HyukjinKwon): Relocate and deduplicate the version specification.
    minimum_pandas_version = "0.23.2"

    from distutils.version import LooseVersion
    try:
        import pandas
        have_pandas = True
    except ImportError:
        have_pandas = False
    if not have_pandas:
        raise ImportError("Pandas >= %s must be installed; however, "
                          "it was not found." % minimum_pandas_version)
    if LooseVersion(pandas.__version__) < LooseVersion(minimum_pandas_version):
        raise ImportError("Pandas >= %s must be installed; however, "
                          "your version was %s." % (minimum_pandas_version, pandas.__version__))


def require_minimum_pyarrow_version():
    """ Raise ImportError if minimum version of pyarrow is not installed
    """
    # TODO(HyukjinKwon): Relocate and deduplicate the version specification.
    minimum_pyarrow_version = "0.12.1"

    from distutils.version import LooseVersion
    try:
        import pyarrow
        have_arrow = True
    except ImportError:
        have_arrow = False
    if not have_arrow:
        raise ImportError("PyArrow >= %s must be installed; however, "
                          "it was not found." % minimum_pyarrow_version)
    if LooseVersion(pyarrow.__version__) < LooseVersion(minimum_pyarrow_version):
        raise ImportError("PyArrow >= %s must be installed; however, "
                          "your version was %s." % (minimum_pyarrow_version, pyarrow.__version__))


def _exception_message(excp):
    """Return the message from an exception as either a str or unicode object.  Supports both
    Python 2 and Python 3.

    >>> msg = "Exception message"
    >>> excp = Exception(msg)
    >>> msg == _exception_message(excp)
    True

    >>> msg = u"unicöde"
    >>> excp = Exception(msg)
    >>> msg == _exception_message(excp)
    True
    """
    if hasattr(excp, "message"):
        return excp.message
    return str(excp)


def _do_server_auth(conn, auth_secret):
    """
    Performs the authentication protocol defined by the SocketAuthHelper class on the given
    file-like object 'conn'.
    """
    write_with_length(auth_secret.encode("utf-8"), conn)
    conn.flush()
    reply = UTF8Deserializer().loads(conn)
    if reply != "ok":
        conn.close()
        raise Exception("Unexpected reply from iterator server.")


def local_connect_and_auth(port):
    """
    Connect to local host, authenticate with it, and return a (sockfile,sock) for that connection.
    Handles IPV4 & IPV6, does some error handling.
    :param port
    :param auth_secret
    :return: a tuple with (sockfile, sock)
    """
    import socket
    sock = None
    errors = []
    # Support for both IPv4 and IPv6.
    # On most of IPv6-ready systems, IPv6 will take precedence.
    for res in socket.getaddrinfo("127.0.0.1", port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, _, sa = res
        try:
            sock = socket.socket(af, socktype, proto)
            sock.settimeout(15)
            sock.connect(sa)
            sockfile = sock.makefile("rwb", int(os.environ.get("BUFFER_SIZE", 65536)))
            return (sockfile, sock)
        except socket.error as e:
            emsg = _exception_message(e)
            errors.append("tried to connect to %s, but an error occured: %s" % (sa, emsg))
            sock.close()
            sock = None
    raise Exception("could not open socket: %s" % errors)
