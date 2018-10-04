package se.lth.cs.docria.values;

import se.lth.cs.docria.Node;
import se.lth.cs.docria.Span;
import se.lth.cs.docria.Value;

public interface ValueVisitor {
    public void accept(Value value);

    public default void accept(BoolValue value) {
        this.accept((Value)value);
    }

    public default void accept(IntValue value) {
        this.accept((Value)value);
    }

    public default void accept(LongValue value) {
        this.accept((Value)value);
    }

    public default void accept(DoubleValue value) {
        this.accept((Value)value);
    }

    public default void accept(NullValue value) {
        this.accept((Value)value);
    }

    public default void accept(BinaryValue value) {
        this.accept((Value)value);
    }

    public default void accept(StringValue value) {
        this.accept((Value)value);
    }

    public default void accept(Span value) {
        this.accept((Value)value);
    }

    public default void accept(Node value) {
        this.accept((Value)value);
    }

    public default void accept(NodeArrayValue value) {
        this.accept((Value)value);
    }
}
