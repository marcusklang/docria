from io import BytesIO, RawIOBase
from msgpack import Unpacker, Packer
from typing import Optional, Callable
import zlib
from typing import Iterator
import struct
from docria.model import Document


class BoundaryReader(RawIOBase):
    def __init__(self, inputio):
        super().__init__()

        self.inputio = inputio  # type: RawIOBase
        self.boundary = self.inputio.read(1)[0]
        if self.boundary < 12 or self.boundary > 30:
            raise IOError("Incorrect boundary value: %d" % self.boundary)

        self.dataread = 1

    def close(self):
        return self.inputio.close()

    def seekable(self):
        return False

    def _compute_length(self, abs_start, abs_stop):
        startseg = abs_start >> self.boundary
        endseg = abs_stop >> self.boundary

        delta = 0

        if abs_start == (abs_start >> self.boundary) << self.boundary:
            delta -= 4

        if abs_stop == (abs_stop >> self.boundary) << self.boundary:
            delta += 4

        return abs_stop-abs_start-(endseg-startseg)*4+delta

    def read(self, n=-1):
        if n == -1:
            alldata = self.inputio.read(n)
        else:
            nearest_boundary = ((self.dataread >> self.boundary) + 1) << self.boundary

            delta = nearest_boundary-self.dataread
            if n < delta:
                alldata = self.inputio.read(n)
            else:
                num_segs = ((n-delta) >> self.boundary)
                n += num_segs*4
                alldata = self.inputio.read(n)

        if len(alldata) == 0:
            return alldata

        # 1. Find nearest upper boundary
        startpos = ((self.dataread >> self.boundary) + 1) << self.boundary
        if startpos - self.dataread == (1 << self.boundary):
            # 1. On the boundary
            pass
        elif startpos - self.dataread >= len(alldata):
            # 2. No boundary in data
            self.dataread += len(alldata)
            return alldata

        k = startpos-self.dataread
        boundarystep = 1 << self.boundary

        output = bytearray(self._compute_length(self.dataread, self.dataread+len(alldata)))
        output[0:startpos] = alldata[0:startpos]

        for i in range(startpos, startpos + len(alldata), boundarystep):
            blockstart = i + 4 - self.dataread
            blockend = min(i + boundarystep - self.dataread, len(alldata))

            output[k:k+boundarystep-4] = alldata[blockstart:blockend]
            k += boundarystep - 4

        self.dataread += len(alldata)

        return bytes(output)

    def readable(self):
        return True

    def writable(self):
        return False


class BoundaryWriter(RawIOBase):
    def __init__(self, outputio, boundary=20, **kwargs):
        super().__init__()
        self.outputio = outputio  # type: RawIOBase
        if 12 <= boundary <= 30:
            self.boundary = boundary
        else:
            raise ValueError("Got invalid boundary value: %d, "
                             "valid value is 12 to 30 which represents 2^12 (4 kiB) to 2^30 (1 GiB)" % boundary)

        self.outputio.write(bytes([boundary]))

        self.written = 1
        self.lastsplit = 1
        self.seg = 0

    def writable(self):
        return True

    def readable(self, *args, **kwargs):
        return False

    def seekable(self, *args, **kwargs):
        return False

    def split(self):
        self.lastsplit = self.written

    def _write_boundary(self):
        assert (self.written >> self.boundary) << self.boundary == self.written
        delta = self.lastsplit - self.written

        deltapos = delta
        if delta <= -0x80000000:
            deltapos = -0x80000000

        self.outputio.write(struct.pack(">i", deltapos))

        self.written += 4
        self.seg += 1

    def write(self, data):
        pos = 0
        left = len(data)

        maxwrite = min(((self.seg + 1) << self.boundary) - self.written, left)
        while left > 0:
            self.outputio.write(data[pos:pos+maxwrite])
            pos += maxwrite
            self.written += maxwrite
            left -= maxwrite

            if left > 0:
                self._write_boundary()
                maxwrite = min(((self.seg + 1) << self.boundary) - self.written, left)

    def close(self):
        self.outputio.close()


class DocumentBlock:
    def __init__(self, unpacker: Unpacker):
        self.unpacker = unpacker

    def __iter__(self):
        return self

    def __next__(self):
        from docria.codec import MsgpackCodec

        data = next(self.unpacker, None)
        if data is None:
            raise StopIteration()
        else:
            return MsgpackCodec.decode(data)


class Codecs:
    NONE = ("none", lambda x: x, lambda x: x)
    DEFLATE = ("zip", lambda x: zlib.compress(x), lambda x: zlib.decompress(x))
    DEFLATE_SQUARED = ("zipsq",
                       lambda x: zlib.compress(zlib.compress(x, level=zlib.Z_BEST_COMPRESSION), level=zlib.Z_BEST_COMPRESSION),
                       lambda x: zlib.decompress(zlib.decompress(x)))


Name2Codec = {
    "none": Codecs.NONE,
    "zip": Codecs.DEFLATE,
    "zipsq": Codecs.DEFLATE_SQUARED
}


class DocumentReader:
    def __init__(self, inputio: RawIOBase):
        self.inputio = inputio
        self.unpacker = Unpacker(self.inputio, raw=False)
        header = self.unpacker.read_bytes(4)
        if header != b"Dmf1":
            raise IOError("Header does not match expected format 'Dmf1', found: %s" % header.decode("latin1"))

        codecname = next(self.unpacker)
        self.codec = Name2Codec.get(codecname, None)
        if self.codec is None:
            raise ValueError("Unsupported codec: %s" % codecname)
        else:
            self.codec = self.codec[2]

        self.num_doc_per_block = next(self.unpacker)
        self.advanced = next(self.unpacker)
        if self.advanced:
            raise NotImplementedError("Advanced mode not implemented.")

        self.block = None  # type: Unpacker

    def __iter__(self)->Iterator[Document]:
        return self

    def __enter__(self)->"DocumentReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.inputio.close()

    def readblock(self)->Optional[Unpacker]:
        data = next(self.unpacker, None)
        if data is None:
            return None
        else:
            buf = self.codec(data)
            unpacker = Unpacker(BytesIO(buf))
            return unpacker

    def __next__(self)->Document:
        if self.block is not None:
            doc = next(self.block, None)
            if doc is None:
                self.block = None
            else:
                return doc

        while self.block is None:
            datablock = self.readblock()
            if datablock is None:
                raise StopIteration()

            self.block = DocumentBlock(datablock)
            doc = next(self.block, None)
            if doc is None:
                self.block = None
            else:
                return doc


class DocumentWriter:
    def __init__(self, outputio: RawIOBase, num_docs_per_block=128, codec=Codecs.DEFLATE, **kwargs):
        self.outputio = outputio
        self.packer = Packer(use_bin_type=True, encoding="utf-8")

        self.outputio.write(b"Dmf1")
        self.outputio.write(self.packer.pack(codec[0]))
        self.outputio.write(self.packer.pack(num_docs_per_block))
        self.outputio.write(self.packer.pack(False))

        if isinstance(self.outputio, BoundaryWriter):
            self.outputio.split()

        self.currentblock = BytesIO()
        self.current_block_count = 0
        self.codec_name = codec[0]
        self.codec = codec[1]  # type: Callable[[bytes], bytes]
        self.num_docs_per_block = num_docs_per_block

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write(self, doc: Document, **kwargs):
        from docria.codec import MsgpackCodec
        binarydata = MsgpackCodec.encode(doc, **kwargs)

        self.currentblock.write(self.packer.pack(binarydata))
        self.current_block_count += 1

        if self.current_block_count == self.num_docs_per_block:
            self.flush()

    def flush(self):
        if self.current_block_count > 0:
            self.outputio.write(self.packer.pack(self.codec(self.currentblock.getvalue())))

            if isinstance(self.outputio, BoundaryWriter):
                self.outputio.split()

            self.currentblock = BytesIO()
            self.current_block_count = 0

    def close(self):
        self.flush()
        self.outputio.close()


class DocumentIO:
    @staticmethod
    def write(filepath, **kwargs)->DocumentWriter:
        return DocumentWriter(BoundaryWriter(open(filepath, "wb"), **kwargs), **kwargs)

    @staticmethod
    def writefile(filelike: RawIOBase, **kwargs)->DocumentWriter:
        return DocumentWriter(BoundaryWriter(filelike, **kwargs), **kwargs)

    @staticmethod
    def read(filepath, **kwargs)->DocumentReader:
        return DocumentReader(BoundaryReader(open(filepath, "rb")))

    @staticmethod
    def readfile(filelike: RawIOBase, **kwargs)->DocumentReader:
        return DocumentReader(BoundaryReader(filelike))
