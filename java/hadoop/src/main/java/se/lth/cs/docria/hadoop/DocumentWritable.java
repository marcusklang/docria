package se.lth.cs.docria.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import se.lth.cs.docria.Document;
import se.lth.cs.docria.MsgpackCodec;
import se.lth.cs.docria.MsgpackDocument;

import java.io.*;

public class DocumentWritable implements Writable, Externalizable {
    private Document document;
    private byte[] msgpackBlob;

    public DocumentWritable() {

    }

    public DocumentWritable(Document document) {
        this.document = document;
    }

    public DocumentWritable(byte[] msgpackBlob) {
        this.msgpackBlob = msgpackBlob;
    }

    public Document getDocument() {
        if(document == null && msgpackBlob != null)
            document = MsgpackCodec.decode(msgpackBlob);

        return document;
    }

    public MsgpackDocument getMsgpackDocument() {
        return new MsgpackDocument(getMsgpackBlob());
    }

    public void setDocument(Document document) {
        this.document = document;
        this.msgpackBlob = null;
    }

    public byte[] getMsgpackBlob() {
        if(this.msgpackBlob == null && this.document == null)
            return null;

        if(msgpackBlob == null) {
            msgpackBlob = MsgpackCodec.encode(this.document).toByteArray();
            document = null;
        }

        return msgpackBlob;
    }

    public void setMsgpackBlob(byte[] msgpackBlob) {
        this.document = null;
        this.msgpackBlob = msgpackBlob;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if(document == null && msgpackBlob == null)
            throw new IllegalStateException("This docria writable does not contain a document, it is null!");

        byte[] msgpackBlob = getMsgpackBlob();

        dataOutput.writeInt(this.msgpackBlob.length);
        dataOutput.write(this.msgpackBlob);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int numBytes = dataInput.readInt();
        byte[] blob = new byte[numBytes];
        dataInput.readFully(blob);

        this.document = null;
        this.msgpackBlob = blob;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readFields(in);
    }
}
