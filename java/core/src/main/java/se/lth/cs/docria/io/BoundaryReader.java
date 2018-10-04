package se.lth.cs.docria.io;

import java.io.*;

public class BoundaryReader extends InputStream {
    private InputStream inputStream;
    private long seg = 0;
    private int boundary;
    private long read = 0;

    public BoundaryReader(InputStream inputStream) throws IOException {
        this.inputStream = inputStream;
        this.boundary = inputStream.read();

        if(this.boundary < 12 || this.boundary > 30)
            throw new IOException("Invalid boundary value: %d, allowed range: [12,30]");

        this.read = 1;
    }

    public static int readBoundary(InputStream inputStream) {
        try {
            int boundary = inputStream.read();

            if(boundary < 12 || boundary > 30)
                throw new IOException("Invalid boundary value: %d, allowed range: [12,30]");

            return boundary;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public static long computeLength(int boundary, long rawStartOffset, long rawStopOffset) {
        long startseg = rawStartOffset >> boundary;
        long endseg = rawStopOffset >> boundary;

        int delta = 0;

        if(rawStartOffset == (rawStartOffset >> boundary) << boundary)
            delta -= 4;

        if(rawStopOffset == (rawStopOffset >> boundary) << boundary)
            delta += 4;

        return rawStopOffset-rawStartOffset-(endseg-startseg)*4+delta;
    }

    @Override
    public int read() throws IOException {
        if(this.read == (seg+1)<<boundary) {
            skipBoundaryValue();
        }

        int value = inputStream.read();
        if(value >= 0)
            this.read += 1;

        return value;
    }

    private void skipBoundaryValue() throws IOException {
        int read = 0;
        while(read < 4) {
            int v = inputStream.read();

            if(read == 0 && v == -1)
                return;
            else if(v == -1)
                throw new EOFException("Reached end before reading a full boundary value!");

            read++;
        }

        this.read += 4;
        seg++;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int left = len;
        int pos = off;

        while(left > 0) {
            if(this.read == (seg+1)<<boundary) {
                skipBoundaryValue();
            }

            int maxread = (int)Math.min(left, ((seg+1)<<boundary) - read);

            int read = inputStream.read(b, pos, maxread);
            if(read >= 0) {
                left -= read;
                pos += read;
                this.read += read;
            } else if(len != left){
                return len-left;
            } else {
                return read;
            }
        }

        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        if(this.read == (seg+1)<<boundary) {
            skipBoundaryValue();
        }

        long maxskip = Math.min(n, ((seg+1)<<boundary) - read);
        return inputStream.skip(maxskip);
    }

    @Override
    public int available() throws IOException {
        if(this.read == (seg+1) << boundary)
            return Math.max(inputStream.available()-4, 0);

        return (int)(Math.min(read + inputStream.available(), (seg + 1) << boundary) - read);
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }


}
