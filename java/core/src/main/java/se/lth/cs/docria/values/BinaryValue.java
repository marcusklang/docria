package se.lth.cs.docria.values;

import se.lth.cs.docria.DataType;
import se.lth.cs.docria.DataTypes;
import se.lth.cs.docria.Value;

import java.util.Base64;

public class BinaryValue extends Value {
    private final byte[] value;

    public BinaryValue(byte[] value) {
        this.value = value;
    }

    @Override
    public DataType type() {
        return DataTypes.BINARY;
    }

    @Override
    public String stringValue() {
        return Base64.getEncoder().encodeToString(value);
    }

    public byte[] binaryValue() {
        return value;
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }
}
