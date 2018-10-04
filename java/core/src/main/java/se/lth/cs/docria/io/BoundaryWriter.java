package se.lth.cs.docria.io;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.IntBuffer;

public class BoundaryWriter extends OutputStream {
    private final OutputStream outputStream;
    private final DataOutputStream boundaryWriter;
    private final long boundary;

    private long written = 0;
    private long seg = 0;
    private long lastsplit = 1;

    public BoundaryWriter(OutputStream outputStream, int boundary) throws IOException {
        if(boundary < 12 || boundary > 30)
            throw new IllegalArgumentException("Boundary must be within 2^12 < 2^boundary < 2^30, which is equivalent to 4 kiB and 1 GiB.");

        this.boundary = boundary;
        this.outputStream = outputStream;
        this.boundaryWriter = new DataOutputStream(outputStream);

        outputStream.write(boundary);
        written += 1;
    }

    public BoundaryWriter(OutputStream outputStream) throws IOException {
        this(outputStream, 18);
    }

    public void split() {
        lastsplit = written;
    }

    private void writeBoundary() throws IOException {
        long delta = lastsplit-written;

        int deltapos = (int)delta;
        if(delta < Integer.MIN_VALUE) {
            deltapos = Integer.MIN_VALUE;
        }

        boundaryWriter.writeInt(deltapos);
        written += 4;
        seg++;
    }

    @Override
    public void write(int b) throws IOException {
        if(((written + 1) >> boundary) > seg) {
            writeBoundary();
        }

        outputStream.write(b);
        written++;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {

        int pos = 0;
        int left = len;

        int maxwrite = (int)Math.min( ((seg + 1) << boundary) - written, left);
        while(left > 0) {
            outputStream.write(b, pos+off, maxwrite);
            pos += maxwrite;
            written += maxwrite;
            left -= maxwrite;

            if(left > 0) {
                writeBoundary();
                maxwrite = (int)Math.min( ((seg + 1) << boundary) - written, left);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        super.flush();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
