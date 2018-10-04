package se.lth.cs.docria.io;

import java.io.IOException;
import java.io.OutputStream;

public class DocumentStreamBoundaryWriter extends DocumentStreamWriter {
    private BoundaryWriter writer;

    public DocumentStreamBoundaryWriter(OutputStream outputStream) throws IOException {
        super(new BoundaryWriter(outputStream));
    }

    public DocumentStreamBoundaryWriter(OutputStream outputStream, int numDocsPerBlock) throws IOException {
        super(new BoundaryWriter(outputStream), numDocsPerBlock);
    }

    public DocumentStreamBoundaryWriter(OutputStream outputStream, Codec codec) throws IOException {
        super(new BoundaryWriter(outputStream), codec);
    }

    public DocumentStreamBoundaryWriter(OutputStream outputStream, int numDocsPerBlock, Codec codec) throws IOException {
        super(new BoundaryWriter(outputStream), numDocsPerBlock, codec);
    }

    public DocumentStreamBoundaryWriter(int boundary, OutputStream outputStream) throws IOException {
        super(new BoundaryWriter(outputStream, boundary));
    }

    public DocumentStreamBoundaryWriter(int boundary, OutputStream outputStream, int numDocsPerBlock) throws IOException {
        super(new BoundaryWriter(outputStream, boundary), numDocsPerBlock);
    }

    public DocumentStreamBoundaryWriter(int boundary, OutputStream outputStream, Codec codec) throws IOException {
        super(new BoundaryWriter(outputStream, boundary), codec);
    }

    public DocumentStreamBoundaryWriter(int boundary, OutputStream outputStream, int numDocsPerBlock, Codec codec) throws IOException {
        super(new BoundaryWriter(outputStream, boundary), numDocsPerBlock, codec);
    }

    @Override
    protected void flushblock() {
        ((BoundaryWriter)outputStream).split();
        super.flushblock();
    }

}
