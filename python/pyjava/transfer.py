"""Bounded Arrow transport primitives shared by Spark readers and Ray servers."""
import io
import os
import tempfile

import pyarrow as pa


def positive_env(name, default):
    value = int(os.environ.get(name, default))
    if value <= 0:
        raise ValueError("%s must be positive" % name)
    return value


class StrictArrowInput(io.RawIOBase):
    """Require an IPC end marker instead of accepting EOF at a batch boundary.

    Arrow reads known-sized messages. A short read is always truncation here;
    a clean stream ends with its zero-length IPC message before any EOF read.
    The caller owns the underlying socket/file.
    """
    def __init__(self, stream):
        super().__init__()
        self.stream = stream

    def readable(self):
        return True

    def read(self, size=-1):
        if size < 0:
            raise ValueError("Arrow transport requires sized reads")
        data = self.stream.read(size)
        if len(data) != size:
            raise EOFError("Truncated Arrow stream: missing data or end marker")
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)


def _size(value):
    # Conservative pre-allocation bound for the supported row values. The
    # resulting RecordBatch is checked again using Arrow's actual byte count.
    if value is None:
        return 8
    if isinstance(value, str):
        return 16 + len(value.encode("utf-8"))
    if isinstance(value, (bytes, bytearray, memoryview)):
        return 16 + len(value)
    if isinstance(value, dict):
        return 16 + sum(_size(v) for v in value.values())
    if isinstance(value, (tuple, list)):
        return 16 + sum(_size(v) for v in value)
    return 32


def arrow_rows(batch):
    """Preserve Arrow scalars without relying on newer RecordBatch.to_pylist."""
    columns = batch.to_pydict()
    names = batch.schema.names
    for values in zip(*(columns[name] for name in names)):
        yield dict(zip(names, values))


def _row_batch(rows, schema):
    # RecordBatch.from_pylist is newer than the project's PyArrow 4.x baseline.
    # Build columns directly, with bounded row lists and no pandas conversion.
    names = schema.names if schema is not None else list(rows[0])
    arrays = [pa.array([row[name] for row in rows],
                       type=schema.field(name).type if schema is not None else None)
              for name in names]
    if schema is not None:
        return pa.RecordBatch.from_arrays(arrays, schema=schema)
    return pa.RecordBatch.from_arrays(arrays, names=names)


def arrow_batches(data, schema=None):
    """Accept row dicts, RecordBatches or Tables; never convert through pandas."""
    max_rows = positive_env("PYJAVA_ARROW_MAX_RECORDS_PER_BATCH", 8192)
    max_bytes = positive_env("PYJAVA_ARROW_MAX_BYTES_PER_BATCH", 8 * 1024 * 1024)
    fields = set(schema.names) if schema is not None else None

    def bounded(batch):
        if batch.num_rows > max_rows:
            for offset in range(0, batch.num_rows, max_rows):
                yield from bounded(batch.slice(offset, max_rows))
        elif batch.nbytes > max_bytes:
            if batch.num_rows <= 1:
                raise ValueError("Single Arrow row exceeds the batch byte budget")
            half = batch.num_rows // 2
            yield from bounded(batch.slice(0, half))
            yield from bounded(batch.slice(half))
        else:
            yield batch

    if isinstance(data, pa.Table):
        data = data.to_batches(max_chunksize=max_rows)
    elif isinstance(data, pa.RecordBatch):
        data = [data]
    rows, size = [], 0
    for item in data:
        if isinstance(item, (pa.RecordBatch, pa.Table)):
            if rows:
                batch = _row_batch(rows, schema)
                schema = batch.schema
                yield from bounded(batch)
                rows, size = [], 0
            batches = item.to_batches(max_chunksize=max_rows) if isinstance(item, pa.Table) else [item]
            for batch in batches:
                if schema is None:
                    schema = batch.schema
                    fields = set(schema.names)
                if not batch.schema.equals(schema):
                    raise ValueError("Arrow schema changed within a partition")
                yield from bounded(batch)
            continue
        if not isinstance(item, dict):
            raise TypeError("Expected a row dict or an Arrow RecordBatch/Table")
        if fields is None:
            fields = set(item)
        elif set(item) != fields:
            raise ValueError("Row fields changed within a partition")
        item_size = _size(item)
        if item_size > max_bytes:
            raise ValueError("Single row exceeds the Arrow batch byte budget")
        if rows and (len(rows) >= max_rows or size + item_size > max_bytes):
            batch = _row_batch(rows, schema)
            schema = batch.schema
            yield from bounded(batch)
            rows, size = [], 0
        rows.append(item)
        size += item_size
    if rows:
        batch = _row_batch(rows, schema)
        schema = batch.schema
        yield from bounded(batch)
    # An explicit schema also preserves the types of an empty partition.
    if schema is None:
        schema = pa.schema([("value", pa.string())])
    yield pa.RecordBatch.from_arrays([pa.array([], type=f.type) for f in schema], schema=schema)


def write_batches(batches, stream):
    batches = iter(batches)
    first = next(batches)
    with pa.ipc.new_stream(stream, first.schema) as writer:
        writer.write_batch(first)
        for batch in batches:
            writer.write_batch(batch)


class ArrowSpool:
    """A committed, quota-limited partition snapshot for retry within one lease."""
    def __init__(self, data, schema=None, cancelled=lambda: False):
        limit = positive_env("PYJAVA_SPOOL_MAX_BYTES", 1024 * 1024 * 1024)
        self.file = tempfile.TemporaryFile(dir=os.environ.get("PYJAVA_SPOOL_DIR"))
        self.size = 0
        try:
            def checked():
                for batch in arrow_batches(data, schema):
                    if cancelled():
                        raise InterruptedError("Partition materialization cancelled")
                    # Include room for IPC metadata; enforce actual file size as well.
                    if self.file.tell() + batch.nbytes + 65536 > limit:
                        raise IOError("Partition exceeds PYJAVA_SPOOL_MAX_BYTES")
                    yield batch
                    if self.file.tell() > limit:
                        raise IOError("Partition exceeds PYJAVA_SPOOL_MAX_BYTES")
            write_batches(checked(), self.file)
            self.size = self.file.tell()
            if self.size > limit:
                raise IOError("Partition exceeds PYJAVA_SPOOL_MAX_BYTES")
        except BaseException:
            self.close()
            raise

    def copy_to(self, out):
        self.file.seek(0)
        while True:
            chunk = self.file.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)

    def close(self):
        self.file.close()
