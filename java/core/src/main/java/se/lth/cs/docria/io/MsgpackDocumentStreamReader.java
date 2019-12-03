/**
 * Copyright 2018 Marcus Klang (marcus.klang@cs.lth.se)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package se.lth.cs.docria.io;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import se.lth.cs.docria.Document;
import se.lth.cs.docria.MsgpackCodec;
import se.lth.cs.docria.MsgpackDocument;

import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MsgpackDocumentStreamReader {
    private InputStream inputStream;
    private MessageUnpacker unpacker;
    private Function<byte[],byte[]> codec;
    private int numDocsPerBlock;

    public long position() {
        return this.unpacker.getTotalReadBytes();
    }

    public MsgpackDocumentStreamReader(InputStream inputStream) {
        this(inputStream, null);
    }

    public MsgpackDocumentStreamReader(InputStream inputStream, Function<byte[],byte[]> codecfn) {
        try {
            this.inputStream = inputStream;
            this.unpacker = MessagePack.newDefaultUnpacker(inputStream);
            byte[] header = this.unpacker.readPayload(4);
            if(!Arrays.equals(header, "Dmf1".getBytes())) {
                throw new IOException("Incorrect header: Dmf1, found: " + new String(header, StandardCharsets.ISO_8859_1));
            }

            if(codecfn != null) {
                this.codec = codecfn;
            } else {
                String codec = this.unpacker.unpackString();
                switch (codec) {
                    case "none":
                        this.codec = x -> x;
                        break;
                    case "zip":
                        this.codec = DeflateCodec::decompress;
                        break;
                    case "zipsq":
                        this.codec = x -> DeflateCodec.decompress(DeflateCodec.decompress(x));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported codec: " + codec);
                }
            }


            this.numDocsPerBlock = this.unpacker.unpackInt();
            boolean advancedMode = this.unpacker.unpackBoolean();
            if(advancedMode) {
                throw new UnsupportedOperationException("Advanced mode is not yet supported");
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private MessageUnpacker block = null;

    private MessageUnpacker readNextBlock() {
        try {
            while (true) {
                int blockSize;
                byte[] compressed;
                synchronized (this) {
                    if(!unpacker.hasNext())
                        return null;

                    blockSize = unpacker.unpackBinaryHeader();
                    compressed = unpacker.readPayload(blockSize);
                }

                byte[] rawdata = codec.apply(compressed);
                block = MessagePack.newDefaultUnpacker(rawdata);
                if(block.hasNext())
                    break;
            }

            return block;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Get next document
     *
     * <b>Remarks: Not safe for concurrent use.</b>
     * @return document
     */
    public MsgpackDocument next() {
        byte[] raw = nextRaw();
        if(raw == null)
            return null;

        return new MsgpackDocument(raw);
    }

    /**
     * Get next document as raw bytes
     *
     * <b>Remarks: Not safe for concurrent use.</b>
     * @return document byte array
     */
    private byte[] nextRaw() {
        try {
            if(block == null) {
                block = readNextBlock();
                if (block == null)
                    return null;
            }

            int docsz = block.unpackBinaryHeader();
            byte[] docdata = block.readPayload(docsz);

            if(!block.hasNext())
                block = null;

            return docdata;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Gets a stream for a single block.
     *
     * <b>Remarks:</b> Safe for concurrent use, only reading is synchronized i.e. will be I/O limited.
     * Deserialization and potential decompression is concurrent.
     * @return null if no more blocks.
     */
    public Stream<MsgpackDocument> nextBlock() {
        final MessageUnpacker unpacker = readNextBlock();
        if(unpacker == null)
            return null;

        Iterator<MsgpackDocument> iterator = new Iterator<MsgpackDocument>() {
            @Override
            public boolean hasNext() {
                try {
                    return unpacker.hasNext();
                } catch (IOException e) {
                    throw new IOError(e);
                }
            }

            @Override
            public MsgpackDocument next() {
                try {
                    int docsz = unpacker.unpackBinaryHeader();
                    byte[] docdata = unpacker.readPayload(docsz);
                    return new MsgpackDocument(docdata);
                } catch (IOException e) {
                    throw new IOError(e);
                }
            }
        };

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.DISTINCT | Spliterator.NONNULL), false);
    }

    public boolean skipBlock() {
        synchronized (this) {
            try {
                if(unpacker.hasNext()) {
                    unpacker.skipValue();
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    }

    public boolean skip() {
        try {
            if(block == null) {
                block = readNextBlock();
                if (block == null)
                    return false;
            }

            block.skipValue();
            if(!block.hasNext())
                block = null;

            return true;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public long skip(long n) {
        long k = 0;
        if(block == null) {
            block = readNextBlock();
            if (block == null)
                return k;
        }

        while(k < n) {
            try {
                block.skipValue();
                k++;

                if(!block.hasNext()) {
                    block = readNextBlock();
                    if (block == null)
                        return k;
                }
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        return k;
    }


    /**
     * Consumes all documents without deserialization for a fast exact document count.
     * @return number of documents found.
     */
    public long count() {
        long k = 0;
        if(block == null) {
            block = readNextBlock();
            if (block == null)
                return k;
        }

        while(true) {
            try {
                block.skipValue();
                k++;

                if(!block.hasNext()) {
                    block = readNextBlock();
                    if (block == null)
                        return k;
                }
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    }

    public void close() {
        try {
            inputStream.close();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
