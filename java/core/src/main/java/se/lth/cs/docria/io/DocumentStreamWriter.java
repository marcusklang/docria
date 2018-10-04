package se.lth.cs.docria.io;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import se.lth.cs.docria.Document;
import se.lth.cs.docria.MsgpackCodec;

import java.io.IOError;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Function;

public class DocumentStreamWriter {
    private Function<byte[],byte[]> codec;
    private int numDocsPerBlock;
    protected OutputStream outputStream;
    private MessagePacker packer;
    private MessageBufferPacker currentblock;
    private int currentblockCount;
    private long numdocs;

    public enum Codec {
        NONE("none", x -> x),
        DEFLATE("zip", DeflateCodec::compress),
        DEFLATE_SQUARED("zipsq", x -> DeflateCodec.compress(DeflateCodec.compress(x)));

        private final Function<byte[],byte[]> codec;
        private final String name;

        Codec(String name, Function<byte[], byte[]> codec) {
            this.name = name;
            this.codec = codec;
        }
    }

    public DocumentStreamWriter(OutputStream outputStream) throws IOException {
        this(outputStream, 128, Codec.DEFLATE);
    }

    public DocumentStreamWriter(OutputStream outputStream, int numDocsPerBlock) throws IOException {
        this(outputStream, numDocsPerBlock, Codec.DEFLATE);
    }

    public DocumentStreamWriter(OutputStream outputStream, Codec codec) throws IOException {
        this(outputStream, 128, codec);
    }

    public DocumentStreamWriter(OutputStream outputStream, int numDocsPerBlock, Codec codec) throws IOException {
        if(numDocsPerBlock < 1)
            throw new IllegalArgumentException("Num docs per block must be >= 1");

        this.numDocsPerBlock = numDocsPerBlock;
        this.outputStream = outputStream;
        this.codec = codec.codec;

        this.packer = MessagePack.newDefaultPacker(outputStream);
        this.packer.addPayload("Dmf1".getBytes());
        this.packer.packString(codec.name);
        this.packer.packInt(numDocsPerBlock);
        this.packer.packBoolean(false);

        this.currentblock = MessagePack.newDefaultBufferPacker();
        this.currentblockCount = 0;
    }

    protected void flushblock() {
        try {
            byte[] encoded = codec.apply(currentblock.toByteArray());
            packer.packBinaryHeader(encoded.length);
            packer.addPayload(encoded);
            packer.flush();

            System.out.println("Flushing block!");

            currentblockCount = 0;
            currentblock.clear();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public void write(Document doc) {
        try {
            byte[] data = MsgpackCodec.encode(doc).toByteArray();
            currentblock.packBinaryHeader(data.length);
            currentblock.addPayload(data);

            numdocs++;

            currentblockCount++;
            if(currentblockCount == numDocsPerBlock) {
                flushblock();
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public void flush() {
        try {
            if(currentblockCount > 0)
                flushblock();

            outputStream.flush();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public void close() {
        try {
            flush();
            outputStream.close();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
