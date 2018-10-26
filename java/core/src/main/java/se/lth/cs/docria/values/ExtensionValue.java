package se.lth.cs.docria.values;

import se.lth.cs.docria.DataType;
import se.lth.cs.docria.DataTypes;
import se.lth.cs.docria.Value;

import java.util.Base64;

public class ExtensionValue extends Value {
    protected String type;
    protected byte[] data;

    public ExtensionValue(String type, byte[] data) {
        this.type = type;
        this.data = data;
    }

    public byte[] binaryValue() {
        return data;
    }

    @Override
    public String stringValue() {
        return type + ":" + Base64.getEncoder().encodeToString(binaryValue());
    }

    @Override
    public DataType type() {
        return DataTypes.ext(type);
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }

    @Override
    public String toString() {
        return "Extension[" + type + "]";
    }
}
