package se.lth.cs.docria.values;

import se.lth.cs.docria.*;

import java.util.Collection;
import java.util.Objects;

public class DocumentValue extends ExtensionValue {
    private byte[] binaryDocument;
    private Document document;

    public DocumentValue(byte[] binaryDocument) {
        super("doc", binaryDocument);
        if(binaryDocument == null)
            throw new NullPointerException("binaryDocument");

        this.binaryDocument = binaryDocument;
    }

    public DocumentValue(Document document) {
        super("doc", null);
        if(document == null)
            throw new NullPointerException("document");

        this.document = document;
    }

    @Override
    public byte[] binaryValue() {
        if(binaryDocument != null)
            return this.binaryDocument;
        else
            return MsgpackCodec.encode(document).toByteArray();
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }

    @Override
    public String stringValue() {
        return Objects.toString(documentValue().maintext());
    }

    public Document documentValue() {
        if(document == null) {
            document = MsgpackCodec.decode(binaryDocument);
            binaryDocument = null;
        }

        return document;
    }

    @Override
    public String toString() {
        return "Document";
    }
}
