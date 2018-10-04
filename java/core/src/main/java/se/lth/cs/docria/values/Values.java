package se.lth.cs.docria.values;

import se.lth.cs.docria.Document;
import se.lth.cs.docria.Node;
import se.lth.cs.docria.Span;
import se.lth.cs.docria.Value;

import java.util.HashMap;
import java.util.Objects;
import java.util.TreeMap;

public class Values {
    public static Value get(boolean value) {
        return value ? BoolValue.TRUE : BoolValue.FALSE;
    }

    public static Value get(int value) {
        return new IntValue(value);
    }

    public static Value get(long value) {
        return new LongValue(value);
    }

    public static Value get(float value) {
        return new DoubleValue(value);
    }

    public static Value get(double value) {
        return new DoubleValue(value);
    }

    public static Value get(byte[] value) {
        return new BinaryValue(value);
    }

    public static Value get(Span value) {
        return value;
    }

    public static Value get(String value) {
        return new StringValue(value);
    }

    public static Value get(Node[] value) {
        return new NodeArrayValue(value);
    }

    public static Value get(Node value) {
        return value;
    }

    public static Value get(Object value) {
        if(value == null) {
            return NullValue.INSTANCE;
        }

        if(value instanceof Value) {
            return (Value)value;
        }

        if(value instanceof Number) {
            if(value instanceof Integer) {
                return get(((Integer) value).intValue());
            } else if(value instanceof Long) {
                return get(((Long) value).longValue());
            } else if(value instanceof Float) {
                return get(((Float) value).floatValue());
            } else if(value instanceof Double) {
                return get(((Double) value).doubleValue());
            } else {
                throw new UnsupportedOperationException("Unsupported value: " + Objects.toString(value));
            }
        } else if(value instanceof String) {
            return get((String) value);
        } else if(value instanceof byte[]) {
            return get((byte[]) value);
        }  else if(value instanceof Boolean) {
            return get(((Boolean) value).booleanValue());
        } else if(value instanceof Node[]) {
            return get((Node[])value);
        } else {
            throw new UnsupportedOperationException("Unsupported value: " + Objects.toString(value));
        }
    }
}
