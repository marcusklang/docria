package se.lth.cs.docria;

import se.lth.cs.docria.exceptions.DataInconsistencyException;
import se.lth.cs.docria.values.ValueVisitor;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class NodeSpan extends Value {
    protected final Node left;
    protected final Node right;

    public NodeSpan(Node left, Node right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        if(left.layer == null) {
            throw new DataInconsistencyException("Left node is not bound to a layer.");
        }
        if(right.layer == null) {
            throw new DataInconsistencyException("Right node is not bound to a layer.");
        }

        if(left.id > right.id)
            throw new DataInconsistencyException(
                    String.format("Left and right nodes are " +
                                  "not in natural sequence: " +
                                  "left.id = %d, right.id = %d", left.id, right.id));

        this.left = left;
        this.right = right;
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }

    @Override
    public Node[] nodeArrayValue() {
        return left.layer.nodespan(left, right).toArray(Node[]::new);
    }

    @Override
    public String stringValue() {
        if(left.has("text") && right.has("text") && left.layer.schema().getFieldType("text").name() == DataTypeName.SPAN) {
            Span startSpan = left.get("text").spanValue();
            Span stopSpan = right.get("text").spanValue();

            int start = startSpan.start();
            int stop = stopSpan.stop();
            return startSpan.source().subSequence(start, stop).toString();
        } else {
            return String.format("NodeSpan(left=%s, right=%s)", left, right);
        }
    }

    @Override
    public NodeSpan nodeSpanValue() {
        return this;
    }

    @Override
    public DataType type() {
        return DataTypes.nodespan(left.layer.name());
    }
}
