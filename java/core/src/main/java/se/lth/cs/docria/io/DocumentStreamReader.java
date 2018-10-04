package se.lth.cs.docria.io;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import se.lth.cs.docria.Document;
import se.lth.cs.docria.MsgpackCodec;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DocumentStreamReader {
    private InputStream inputStream;
    private MessageUnpacker unpacker;
    private Function<byte[],byte[]> codec;
    private int numDocsPerBlock;

    public DocumentStreamReader(InputStream inputStream) {
        try {
            this.inputStream = inputStream;
            this.unpacker = MessagePack.newDefaultUnpacker(inputStream);
            byte[] header = this.unpacker.readPayload(4);
            if(!Arrays.equals(header, "Dmf1".getBytes())) {
                throw new IOException("Incorrect header: Dmf1, found: " + new String(header, StandardCharsets.ISO_8859_1));
            }

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
    public Document next() {
        byte[] raw = nextRaw();
        if(raw == null)
            return null;

        return MsgpackCodec.decode(raw);
    }

    /**
     * Get next document as raw bytes
     *
     * <b>Remarks: Not safe for concurrent use.</b>
     * @return document byte array
     */
    public byte[] nextRaw() {
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
    public Stream<Document> nextBlock() {
        final MessageUnpacker unpacker = readNextBlock();
        if(unpacker == null)
            return null;

        Iterator<Document> iterator = new Iterator<Document>() {
            @Override
            public boolean hasNext() {
                try {
                    return unpacker.hasNext();
                } catch (IOException e) {
                    throw new IOError(e);
                }
            }

            @Override
            public Document next() {
                try {
                    int docsz = unpacker.unpackBinaryHeader();
                    byte[] docdata = unpacker.readPayload(docsz);
                    return MsgpackCodec.decode(docdata);
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
