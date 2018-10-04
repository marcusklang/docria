package se.lth.cs.docria.values;

import se.lth.cs.docria.DataType;
import se.lth.cs.docria.DataTypes;
import se.lth.cs.docria.Value;

public class DoubleValue extends Value {
    private final double value;

    public double EPS = 1e-12;

    @Override
    public DataType type() {
        return DataTypes.FLOAT_64;
    }

    public DoubleValue(double value) {
        this.value = value;
    }

    @Override
    public String stringValue() {
        return null;
    }

    @Override
    public boolean boolValue() {
        return Math.abs(value) - EPS <= 0.0;
    }

    @Override
    public int intValue() {
        return (int)value;
    }

    @Override
    public long longValue() {
        return (long)value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }
}
