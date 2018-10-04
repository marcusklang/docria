package se.lth.cs.docria.values;

import se.lth.cs.docria.DataType;
import se.lth.cs.docria.Value;

public class NullValue extends Value {
    private NullValue() {

    }

    public static NullValue INSTANCE = new NullValue();

    @Override
    public DataType type() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String stringValue() {
        return "";
    }

    @Override
    public boolean boolValue() {
        return false;
    }

    @Override
    public int intValue() {
        return 0;
    }

    @Override
    public long longValue() {
        return 0;
    }

    @Override
    public double doubleValue() {
        return 0.0;
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }
}
