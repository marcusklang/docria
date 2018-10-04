package se.lth.cs.docria;

import se.lth.cs.docria.values.ValueVisitor;

public abstract class Value {

    public abstract String stringValue();

    public abstract DataType type();

    public boolean boolValue() {
        return Boolean.parseBoolean(stringValue());
    }

    public int intValue() {
        return Integer.parseInt(stringValue());
    }

    public long longValue() {
        return Long.parseLong(stringValue());
    }

    public double doubleValue() {
        return Double.parseDouble(stringValue());
    }

    public byte[] binaryValue() {
        throw new UnsupportedOperationException();
    }

    public Node nodeValue() {
        throw new UnsupportedOperationException();
    }

    public Node[] nodeArrayValue() {
        throw new UnsupportedOperationException();
    }

    public Span spanValue() {
        throw new UnsupportedOperationException();
    }

    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }
}
