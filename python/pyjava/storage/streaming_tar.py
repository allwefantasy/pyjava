import io
import os
import tarfile
import tempfile

import uuid

BLOCK_SIZE = 1024 * 64


class FileStream(object):
    def __init__(self):
        self.buffer = io.BytesIO()
        self.offset = 0

    def write(self, s):
        self.buffer.write(s)
        self.offset += len(s)

    def tell(self):
        return self.offset

    def close(self):
        self.buffer.close()

    def pop(self):
        s = self.buffer.getvalue()
        self.buffer.close()
        self.buffer = io.BytesIO()
        return s


def build_rows_from_file(target_dir):
    file = FileStream()
    start = 0
    for i in stream_build_tar(target_dir, file):
        v = file.pop()
        offset = len(v)
        if len(v) > 0:
            yield {"start": start, "offset": offset, "value": v}
            start = start + offset


def save_rows_as_file(data, target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    tf_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
    with open(tf_path, "wb") as tf:
        for block_row in data:
            tf.write(block_row["value"])
    with open(tf_path, "rb") as tf:
        tt = tarfile.open(tf.name, mode="r:")
        tt.extractall(target_dir)
        tt.close()
    os.remove(tf_path)


def stream_build_tar(file_dir, streaming_fp):
    tar = tarfile.TarFile.open(fileobj=streaming_fp, mode="w:")
    for root, dirs, files in os.walk(file_dir):
        for in_filename in files:
            stat = os.stat(os.path.join(root, in_filename))
            tar_info = tarfile.TarInfo(os.path.join(root.lstrip(file_dir), in_filename))
            # tar_info.path = root.lstrip(file_dir)
            tar_info.mtime = stat.st_mtime
            tar_info.size = stat.st_size

            tar.addfile(tar_info)

            yield

            with open(os.path.join(root, in_filename), 'rb') as in_fp:
                # total_size = 0
                while True:
                    s = in_fp.read(BLOCK_SIZE)

                    if len(s) > 0:
                        tar.fileobj.write(s)
                        yield

                    if len(s) < BLOCK_SIZE:
                        blocks, remainder = divmod(tar_info.size, tarfile.BLOCKSIZE)

                        if remainder > 0:
                            tar.fileobj.write(tarfile.NUL *
                                              (tarfile.BLOCKSIZE - remainder))

                            yield

                            blocks += 1

                        tar.offset += blocks * tarfile.BLOCKSIZE
                        break
    tar.close()


def main():
    rows = build_rows_from_file("/Users/allwefantasy/data/mlsql/homes/demo/tmp/minist_model")
    save_rows_as_file(rows, "/Users/allwefantasy/data/mlsql/homes/demo/tmp/minist_model3")


if __name__ == '__main__':
    main()
