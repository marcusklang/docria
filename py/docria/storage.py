# -*- coding: utf-8 -*-
#
# Copyright 2018 Marcus Klang (marcus.klang@cs.lth.se)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
from io import BytesIO, RawIOBase, SEEK_SET, SEEK_CUR, SEEK_END
from msgpack import Unpacker, Packer
from typing import Optional, Callable
import zlib
from typing import Iterator
import struct
from docria.model import Document


class BoundaryReader(RawIOBase):
    """Note: Seek is not ready for prime time yet, unresolved bugs."""
    def __init__(self, inputio):
        super().__init__()

        self.inputio = inputio  # type: RawIOBase
        self.boundary = self.inputio.read(1)[0]
        if self.boundary < 12 or self.boundary > 30:
            raise IOError("Incorrect boundary value: %d" % self.boundary)

        self.offset = 0

    def close(self):
        return self.inputio.close()

    def seekable(self):
        return self.inputio.seekable()

    def _to_absolute(self, offset):
        boundary_val = 1 << self.boundary
        if offset < boundary_val - 1:
            return offset + 1
        else:
            upper = offset - boundary_val + 1
            return (upper // (boundary_val-4))*4 + boundary_val + 4 + upper

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        if whence != SEEK_SET:
            raise NotImplementedError()

        self.offset = offset
        abs_start_position = self._to_absolute(offset)
        pos = self.inputio.seek(abs_start_position, SEEK_SET)
        assert pos == abs_start_position
        return offset

    def read(self, n=-1):
        abs_start_position = ((self.offset + 1) >> self.boundary)*4+(self.offset + 1)
        if n == -1:
            alldata = self.inputio.read(n)
            abs_stop_position = abs_start_position + len(alldata)
        else:
            abs_stop_position = ((self.offset + n + 1) >> self.boundary)*4+(self.offset + n + 1)
            alldata = self.inputio.read(abs_stop_position-abs_start_position)
            abs_stop_position = abs_start_position + len(alldata)

        if len(alldata) == 0:
            return alldata

        if (abs_start_position >> self.boundary) << self.boundary == abs_start_position:
            abs_start_position += 4

        num_boundaries = (abs_stop_position >> self.boundary) - (abs_start_position >> self.boundary)
        real_length = abs_stop_position-abs_start_position-num_boundaries*4

        if (abs_stop_position >> self.boundary) << self.boundary == abs_stop_position:
            real_length -= 4

        output = bytearray(real_length)

        output_position = 0
        rel_position = 0
        current_position = abs_start_position

        while current_position < abs_stop_position:
            max_read = min(((current_position >> self.boundary) + 1) << self.boundary, abs_stop_position)-current_position

            output[output_position:output_position+max_read] = alldata[rel_position:rel_position+max_read]
            rel_position += 4 + max_read
            current_position += max_read + 4
            output_position += max_read

        assert output_position == real_length
        self.offset += real_length

        return bytes(output)

    def tell(self):
        return self.inputio.tell()-(self.inputio.tell() >> self.boundary)*4-1

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
    def __init__(self, position: int, rawbuffer: bytes):
        self._dataread = 0
        self._data = BytesIO(rawbuffer)
        self._unpacker = Unpacker(self._data, raw=False)
        self._position = position

    @property
    def position(self)->int:
        return self._position

    def tell(self)->int:
        return self._unpacker.tell()+self._dataread

    def seek(self, position)->int:
        pos = self._data.seek(position)
        self._dataread = position
        self._unpacker = Unpacker(self._data, raw=False)
        return pos

    def documents(self):
        docs = []

        last = 0
        self.seek(0)
        for doc in self:
            docs.append((last, doc))
            last = self.tell()

        return docs

    def __iter__(self):
        return self

    def __next__(self):
        from docria.codec import MsgpackDocument

        blockpos = self.tell()
        data = next(self._unpacker, None)
        if data is None:
            raise StopIteration()
        else:
            return MsgpackDocument(data, ref=(self._position, blockpos))


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
        self.dataread = 4
        header = self.inputio.read(4)
        if header != b"Dmf1":
            raise IOError("Header does not match expected format 'Dmf1', found: %s" % header.decode("latin1"))

        self.unpacker = Unpacker(self.inputio, raw=False)

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

        self.block = None  # type: DocumentBlock

    def __iter__(self)->Iterator[Document]:
        return self

    def __enter__(self)->"DocumentReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.inputio.close()

    def get(self, ref):
        if self.block is None or self.block.position != ref[0]:
            self.seek(ref[0])
            self.block = self.readblock()

        self.block.seek(ref[1])
        return next(self.block)

    def seek(self, position):
        self.block = None
        self.inputio.seek(position, SEEK_SET)
        self.unpacker = Unpacker(self.inputio, raw=False)
        self.dataread = position

    def blocks(self):
        while True:
            bl = self.readblock()
            if bl is None:
                return
            else:
                yield bl

    def readblock(self)->Optional[DocumentBlock]:
        pos = self.unpacker.tell() + self.dataread
        data = next(self.unpacker, None)
        if data is None:
            return None
        else:
            buf = self.codec(data)
            blk = DocumentBlock(pos, buf)
            return blk

    def __next__(self)->Document:
        from docria.codec import MsgpackCodec
        if self.block is not None:
            doc = next(self.block._unpacker, None)
            if doc is None:
                self.block = None
            else:
                return MsgpackCodec.decode(doc)

        while self.block is None:
            datablock = self.readblock()
            if datablock is None:
                raise StopIteration()

            self.block = datablock
            doc = next(self.block._unpacker, None)
            if doc is None:
                self.block = None
            else:
                return MsgpackCodec.decode(doc)

    def close(self):
        self.inputio.close()


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
        return DocumentWriter(open(filepath, "wb"), **kwargs)

    @staticmethod
    def writefile(filelike: RawIOBase, **kwargs)->DocumentWriter:
        return DocumentWriter(filelike, **kwargs)

    @staticmethod
    def read(filepath, **kwargs)->DocumentReader:
        return DocumentReader(open(filepath, "rb"))

    @staticmethod
    def readfile(filelike: RawIOBase, **kwargs)->DocumentReader:
        return DocumentReader(filelike)


class DocumentFileIndex:
    """In-memory index of a single docria file"""
    def __init__(self, filepath, properties, docs):
        self.filepath = filepath
        self.properties = properties
        self.docs = docs

    @staticmethod
    def build(source_filepath, *properties, **kwargs):
        reader = DocumentIO.read(source_filepath, **kwargs)
        property2docis = {prop: {} for prop in properties}
        docs = []

        for docid, doc in enumerate(doc for block in reader.blocks() for doc in block):
            docs.append(doc.ref)
            props = doc.properties()
            for prop in properties:
                if prop in props:
                    property2docis[prop].setdefault(props[prop], []).append(docid)

        reader.close()

        return DocumentFileIndex(source_filepath, property2docis, docs)

    def search(self, conds, lazy=False):
        if len(conds) == 0:
            return

        all_hits = []
        for k, v in dict(conds).items():
            all_hits.append(set(self.properties.get(k, {}).get(v, [])))

        results = all_hits[0]
        for s in all_hits[1:]:
            results.intersection_update(s)

        from docria.codec import MsgpackCodec

        if len(results) > 0:
                reader = DocumentIO.read(self.filepath)
                lastblk = None
                for docid in sorted(results):
                    ref = self.docs[docid]
                    try:
                        if lastblk is None:
                            reader.seek(ref[0])
                            lastblk = reader.readblock()
                        elif lastblk.position != ref[0]:
                            reader.seek(ref[0])
                            lastblk = reader.readblock()

                        lastblk.seek(ref[1])
                        if lazy:
                            yield next(lastblk)
                        else:
                            yield MsgpackCodec.decode(next(lastblk._unpacker))
                    except Exception as e:
                        raise IOError("Failed to read document in %s for ref %d, %d" % (self.filepath, ref[0], ref[1]))


class DocumentIndex:
    """Simple In-Memory Index for debugging"""
    def __init__(self, basepath="."):
        self.basepath = os.path.abspath(basepath)
        self.index = {} # type: Dict[str, DocumentFileIndex]

    def add(self, index: "DocumentFileIndex"):
        index.filepath = os.path.relpath(index.filepath, self.basepath)
        self.index[index.filepath] = index

    def search(self, conds, lazy=False):
        for indx in self.index.values():
            for doc in indx.search(conds, lazy=lazy):
                yield doc

    def save(self, path):
        import pickle
        with open(path, "wb") as fout:
            pickle.dump(self, fout, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load(self, path):
        import pickle
        with open(path, "rb") as fin:
            return pickle.load(fin)
