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
"""I/O module, read/write collections of documents"""
import os
from io import BytesIO, RawIOBase, SEEK_SET, SEEK_CUR, SEEK_END
from msgpack import Unpacker, Packer
from typing import Optional, Callable, Dict, Union, List, Tuple
import zlib
from typing import Iterator
import struct
import importlib
from docria.model import Document
from docria.codec import MsgpackDocument
import tarfile
import time


def _module_available(name):
    return importlib.util.find_spec(name) is not None


class _BoundaryReader(RawIOBase):
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


class _BoundaryWriter(RawIOBase):
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


class MsgpackDocumentBlock:
    """
    Represents a block of MessagePack docria documents

    .. automethod:: __iter__
    .. automethod:: __next__
    """
    def __init__(self, position: int, rawbuffer: bytes):
        self._dataread = 0
        self._data = BytesIO(rawbuffer)
        self._unpacker = Unpacker(self._data, raw=False)
        self._position = position

    @property
    def position(self)->int:
        """Get the original byte position"""
        return self._position

    def tell(self)->int:
        """Get the current byte position within this block"""
        return self._unpacker.tell()+self._dataread

    def seek(self, position)->int:
        pos = self._data.seek(position)
        self._dataread = position
        self._unpacker = Unpacker(self._data, raw=False)
        return pos

    def documents(self)->List[Tuple[int, MsgpackDocument]]:
        """Return all documents as a list of tuples (position, MessagePack Docria document)"""
        docs = []

        last = 0
        self.seek(0)
        for doc in self:
            docs.append((last, doc))
            last = self.tell()

        return docs

    def __iter__(self):
        """:returns: self"""
        return self

    def __next__(self):
        """:returns: MsgpackDocument with the encoded document"""
        from docria.codec import MsgpackDocument

        blockpos = self.tell()
        data = next(self._unpacker, None)
        if data is None:
            raise StopIteration()
        else:
            return MsgpackDocument(data, ref=(self._position, blockpos))


class BlockCodecs:
    """Block compression codecs"""
    NONE = ("none", lambda x: x, lambda x: x)
    DEFLATE = ("zip", lambda x: zlib.compress(x), lambda x: zlib.decompress(x))
    DEFLATE_SQUARED = ("zipsq",
                       lambda x: zlib.compress(zlib.compress(x, level=zlib.Z_BEST_COMPRESSION)
                                               , level=zlib.Z_BEST_COMPRESSION),
                       lambda x: zlib.decompress(zlib.decompress(x)))


_Name2Codec = {
    "none": BlockCodecs.NONE,
    "zip": BlockCodecs.DEFLATE,
    "zipsq": BlockCodecs.DEFLATE_SQUARED
}


class MsgpackDocumentReader:
    """Reader for the blocked MessagePack document file format"""
    def __init__(self, inputio: RawIOBase):
        self.inputio = inputio
        self.dataread = 4
        header = self.inputio.read(4)
        if header != b"Dmf1":
            raise IOError("Header does not match expected format 'Dmf1', found: %s" % header.decode("latin1"))

        self.unpacker = Unpacker(self.inputio, raw=False)

        codecname = next(self.unpacker)
        self.codec = _Name2Codec.get(codecname, None)
        if self.codec is None:
            raise ValueError("Unsupported codec: %s" % codecname)
        else:
            self.codec = self.codec[2]

        self.num_doc_per_block = next(self.unpacker)
        self.advanced = next(self.unpacker)
        if self.advanced:
            raise NotImplementedError("Advanced mode not implemented.")

        self.block = None  # type: MsgpackDocumentBlock
        self._lastblockpos = inputio.tell()

    def __iter__(self)->Iterator[MsgpackDocument]:
        return self

    def __enter__(self)-> "MsgpackDocumentReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.inputio.close()

    def get(self, ref):
        """
        Returns a specific document at position (file position, block position)

        :param ref: tuple of (raw file position, uncompressed block position)

        :return: MessagePack document instance

        :note:
        This method assumes and requires that the underlying I/O supports seeking.
        """
        if self.block is None or self.block.position != ref[0]:
            self.seek(ref[0])
            self.block = self.readblock()

        self.block.seek(ref[1])
        return next(self.block)

    def seek(self, position):
        """
        Seek to a block position

        :param position: raw file position

        :note:
        This method assumes and requires that the underlying I/O supports seeking.
        """
        self.block = None
        self.inputio.seek(position, SEEK_SET)
        self.unpacker = Unpacker(self.inputio, raw=False)
        self.dataread = position

    def blocks(self):
        """Get iterator for all document blocks"""
        while True:
            bl = self.readblock()
            if bl is None:
                return
            else:
                yield bl

    def readblock(self)->Optional[MsgpackDocumentBlock]:
        """Read a single block if possible"""
        self._lastblockpos = self.unpacker.tell() + self.dataread
        data = next(self.unpacker, None)
        if data is None:
            return None
        else:
            buf = self.codec(data)
            blk = MsgpackDocumentBlock(self._lastblockpos, buf)
            return blk

    def __next__(self)->MsgpackDocument:
        if self.block is not None:
            start = self.block.tell()
            doc = next(self.block._unpacker, None)
            if doc is None:
                self.block = None
            else:
                return MsgpackDocument(doc, ref=(self._lastblockpos, start))

        while self.block is None:
            datablock = self.readblock()
            if datablock is None:
                raise StopIteration()

            self.block = datablock
            start = self.block.tell()
            doc = next(self.block._unpacker, None)
            if doc is None:
                self.block = None
            else:
                return MsgpackDocument(doc, ref=(self._lastblockpos, start))

    def close(self):
        self.inputio.close()


class DocumentReader:
    """Utility reader, returns Docria documents."""
    def __init__(self, inputreader):
        self.inputreader = inputreader

    def __enter__(self)-> "MsgpackDocumentReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.inputreader.close()

    def __iter__(self):
        return self

    def __next__(self):
        doc = next(self.inputreader, None)
        if doc is None:
            raise StopIteration()

        return doc.document()


class MsgpackDocumentWriter:
    """Writer for the blocked MessagePack document file format"""
    def __init__(self, outputio: RawIOBase, num_docs_per_block=128, codec=BlockCodecs.DEFLATE, **kwargs):
        """
        Primary constructor

        :param outputio: the underlying I/O device to write to.
        :param num_docs_per_block: the number of documents to cache before
                                   compressing the entire block and write to underlying storage.
        :param codec: the compression codec to use for blocks
        """
        self.outputio = outputio
        self.packer = Packer(use_bin_type=True)

        self.outputio.write(b"Dmf1")
        self.outputio.write(self.packer.pack(codec[0]))
        self.outputio.write(self.packer.pack(num_docs_per_block))
        self.outputio.write(self.packer.pack(False))

        if isinstance(self.outputio, _BoundaryWriter):
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
        """
        Write docria document

        :param doc: accepts unencoded Document or Messagepack Document for fast writing
        :param kwargs: options to pass to :meth:`docria.codec.MsgpackCodec.encode`
        """
        from docria.codec import MsgpackCodec

        if isinstance(doc, Document):
            data = MsgpackCodec.encode(doc, **kwargs)
        elif isinstance(doc, MsgpackDocument):
            data = doc.rawdata.getvalue()
        else:
            raise ValueError("Got unsupported doc, only Document and MsgpackDocument allowed")

        self.currentblock.write(self.packer.pack(data))
        self.current_block_count += 1

        if self.current_block_count == self.num_docs_per_block:
            self.flush()

    def flush(self):
        """
        Flush data to the underlying storage.

        :note:
        Will force currently cached blocks to be compressed and written to disk.
        This might result in blocks having less than specified number of documents per block.
        """
        if self.current_block_count > 0:
            self.outputio.write(self.packer.pack(self.codec(self.currentblock.getvalue())))

            if isinstance(self.outputio, _BoundaryWriter):
                self.outputio.split()

            self.currentblock = BytesIO()
            self.current_block_count = 0

    def close(self):
        """Flush data and close the underlying storage"""
        self.flush()
        self.outputio.close()


class TarMsgpackReader:
    """Reader for the tar-based sequential MessagePack format."""
    def __init__(self, inputpath, mode="r|gz", **kwargs):
        """
        TarMsgpackReader constructor

        :param inputpath: filepath to tar
        :param mode: the tarball reading mode, :meth:`tarfile.open`, \
                     can be used to select bz2 \or lzma compression modes.
        """
        self.tarreader = tarfile.open(inputpath, mode=mode)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tarreader.close()

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            ti = self.tarreader.next()  # type: tarfile.TarInfo
            if ti is None:
                raise StopIteration

            if ti.isfile():
                obj = self.tarreader.extractfile(ti)
                return MsgpackDocument(obj.read())

    def close(self):
        self.tarreader.close()


class TarMsgpackWriter:
    """Writer for the tar-based sequential MessagePack format."""
    def __init__(self, outputpath, docformat="doc%05d.msgpack", rootdir=None, mode="w|gz", **kwargs):
        """
        TarMsgpackWriter

        :param outputpath: filepath to tar
        :param docformat: naming convention of files in the tarball,
        must include a single digit using old-style string formatting.
        :param rootdir: set to string if a root directory within the tarfile should be used.
        :param mode: the tarball writing mode, :meth:`tarfile.open`, \
                     can be used to select bz2 or lzma compression modes.
        """
        self.tarwriter = tarfile.open(outputpath, mode=mode, **kwargs)
        self.rootdir = rootdir
        self.docformat = docformat
        if rootdir is not None:
            ti = tarfile.TarInfo(rootdir)
            ti.type = tarfile.DIRTYPE
            ti.mtime = time.time()

            self.tarwriter.addfile(ti)

        self.i = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write(self, doc: Union[Document, MsgpackDocument]):
        """
        Write document

        :param doc: accepts unencoded Document, and encoded MsgpackDocument for fast conversion.
        """
        from docria.codec import MsgpackCodec
        if self.rootdir is not None:
            ti = tarfile.TarInfo(os.path.join(self.rootdir, self.docformat % self.i))
        else:
            ti = tarfile.TarInfo(self.docformat % self.i)

        ti.mtime = time.time()

        if isinstance(doc, Document):
            data = MsgpackCodec.encode(doc)
        elif isinstance(doc, MsgpackDocument):
            data = doc.rawdata.getvalue()
        else:
            raise ValueError("Got unsupported doc, only Document and MsgpackDocument allowed")

        ti.size = len(data)
        self.i += 1

        self.tarwriter.addfile(ti, fileobj=BytesIO(data))

    def close(self):
        self.tarwriter.close()


class DocumentIO:
    """
    .. deprecated::
        Use concrete variants instead such as MsgpackDocumentIO
    """
    @staticmethod
    def write(filepath, **kwargs)->MsgpackDocumentWriter:
        return MsgpackDocumentWriter(open(filepath, "wb"), **kwargs)

    @staticmethod
    def writefile(filelike: RawIOBase, **kwargs)->MsgpackDocumentWriter:
        return MsgpackDocumentWriter(filelike, **kwargs)

    @staticmethod
    def read(filepath, **kwargs)->DocumentReader:
        return DocumentIO.readfile(open(filepath, "rb"), **kwargs)

    @staticmethod
    def readfile(filelike: RawIOBase, **kwargs)->DocumentReader:
        return DocumentReader(MsgpackDocumentReader(filelike))


class MsgpackDocumentIO:
    @staticmethod
    def read(filepath, **kwargs)->MsgpackDocumentReader:
        return MsgpackDocumentIO.readfile(open(filepath, "rb"))

    @staticmethod
    def readfile(filelike, **kwargs)->MsgpackDocumentReader:
        return MsgpackDocumentReader(filelike)


class DocumentFileIndex:
    """In-memory index of a single docria file"""
    def __init__(self, filepath: str,
                 properties: Dict[str, Dict[any, List[int]]],
                 docrefs: List[Tuple[int, int]]):
        """
        Constructor of DocumetnFileIndex

        :param filepath: path to MessagePack Document file
        :param properties: the property index, dictin
        :param docrefs: list of document references
        """
        self.filepath = filepath
        self.properties = properties
        self.docrefs = docrefs

    @staticmethod
    def build(source_filepath, *properties, **kwargs):
        reader = MsgpackDocumentIO.read(source_filepath, **kwargs)
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
            reader = MsgpackDocumentIO.read(self.filepath)
            lastblk = None

            # Optimized reading if multiple hits exist within a block sequentially
            for docid in sorted(results):
                ref = self.docrefs[docid]
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
                    raise IOError("Failed to read document in %s "
                                  "for ref %d, %d" % (self.filepath, ref[0], ref[1])) from e


class DocumentIndex:
    """Multi-file in-memory index"""
    def __init__(self, basepath="."):
        self.basepath = os.path.abspath(basepath)
        self.index = {}  # type: Dict[str, DocumentFileIndex]

    def add(self, index: "DocumentFileIndex"):
        index.filepath = os.path.relpath(index.filepath, self.basepath)
        self.index[index.filepath] = index

    def search(self, conds, lazy=False):
        for indx in self.index.values():
            for doc in indx.search(conds, lazy=lazy):
                yield doc

    def save(self, path):
        """Save index as a pickle file"""
        import pickle
        with open(path, "wb") as fout:
            pickle.dump(self, fout, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load(path):
        """Load pickle index"""
        import pickle
        with open(path, "rb") as fin:
            return pickle.load(fin)


def _build_file_index(args):
    path = args["path"]
    props = args["props"]
    return DocumentFileIndex.build(path, *props)


def build_msgpack_fileindex(path, *props)->"DocumentFileIndex":
    """
    Construct a document index

    :param path: path to file which can be read by :class:`~docria.storage.MsgpackDocumentReader`
    :param props: the properties to index

    :return: built index
    """
    return DocumentFileIndex.build(path, *props)


def build_msgpack_directory_fileindex(path, *props, basepath=".", num_workers=None)->"DocumentIndex":
    """
    Construct a document index spanning over multiple docria files.

    :param path: path to the directory containing docria files
    :param props: the properties to index
    :param basepath: the relative path to use when saving filepath locations
    :param num_workers: the number of processes to spawn for multicore processing of files,
    default is the number of cores available as given by :meth:`multiprocessing.cpu_count`.

    :return: populated DocumentIndex

    :note:
    basepath can be used to create an index which only has relative references and thus can be
    included with the document collection.
    """
    from multiprocessing import Pool, cpu_count

    docria_files = [os.path.join(path, fpath) for fpath in os.listdir(path)
                    if not fpath.startswith(".") and fpath.lower().endswith(".docria")]
    master_indx = DocumentIndex(basepath=basepath)

    proplist = list(props)
    num_workers = cpu_count() if num_workers is None else num_workers

    with Pool(processes=num_workers) as p:
        if _module_available("tqdm"):
            from tqdm import tqdm

            with tqdm("Building index", total=len(docria_files)) as pbar:
                for indxitem in p.imap_unordered(_build_file_index, [{"path": path, "props": proplist}
                                                                     for path in docria_files]):
                    master_indx.add(indxitem)
                    pbar.update(1)

        else:
            for indxitem in p.imap_unordered(_build_file_index, [{"path": path, "props": proplist}
                                                                 for path in docria_files]):
                master_indx.add(indxitem)

    return master_indx
