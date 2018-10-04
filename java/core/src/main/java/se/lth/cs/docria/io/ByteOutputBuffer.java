package se.lth.cs.docria.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;

public class ByteOutputBuffer extends OutputStream {
    private final int buffersize;
    private ArrayList<byte[]> buffers = new ArrayList<>();
    private byte[] current;
    private int pos = 0;
    private int written = 0;

    public ByteOutputBuffer() {
        this.buffersize = 1024;
        this.current = new byte[this.buffersize];
        buffers.add(this.current);
    }

    public ByteOutputBuffer(int buffersize) {
        this.buffersize = buffersize;
        this.current = new byte[this.buffersize];
        buffers.add(this.current);
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void write(int b) throws IOException {
        if(pos == current.length) {
            current = new byte[buffersize];
            buffers.add(current);
            pos = 0;
        }

        current[pos++] = (byte)(b & 0xFF);
        written++;
    }

    public void write(byte[] data) {
        write(data, 0, data.length);
    }

    public void write(byte[] data, int offset, int length) {
        if(pos == current.length) {
            current = new byte[buffersize];
            buffers.add(current);
            pos = 0;
        }

        int left = length;
        while(left > 0) {
            int maxwrite = Math.min(current.length-pos, left);

            System.arraycopy(data, offset+(length-left), current, pos, maxwrite);
            left -= maxwrite;
            pos += maxwrite;
            written += maxwrite;

            if(pos == current.length && left > 0) {
                current = new byte[buffersize];
                buffers.add(current);
                pos = 0;
            }
        }
    }

    public int length() {
        return written;
    }

    public byte[] toByteArray() {
        byte[] output = new byte[written];
        int pos = 0;
        int left = written;
        Iterator<byte[]> iter = buffers.iterator();

        while(left > 0) {
            byte[] buffer = iter.next();
            int write = Math.min(buffer.length, left);
            System.arraycopy(buffer, 0, output, pos, write);
            left -= write;
            pos += write;
        }

        return output;
    }
}
