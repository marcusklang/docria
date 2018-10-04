package se.lth.cs.docria;

import jdk.nashorn.internal.runtime.arrays.ArrayIndex;
import se.lth.cs.docria.values.ValueVisitor;

import java.util.stream.IntStream;

public class Span extends Value implements CharSequence, Comparable<Span> {
    private Text parent;
    private Offset startOffset;
    private Offset stopOffset;

    public Text parent() {
        return parent;
    }

    public Span(Text parent, Offset startOffset, Offset stopOffset) {
        this.parent = parent;
        this.startOffset = startOffset;
        this.stopOffset = stopOffset;
    }

    protected void incref() {
        startOffset.refcount += 1;
        stopOffset.refcount += 1;
    }

    @Override
    public DataType type() {
        return parent.spanType();
    }

    public int start() {
        return startOffset.offset;
    }

    public int stop() {
        return stopOffset.offset;
    }

    public Offset startOffset() {
        return startOffset;
    }

    public Offset stopOffset() {
        return stopOffset;
    }

    @Override
    public String stringValue() {
        return toString();
    }

    @Override
    public Span spanValue() {
        return this;
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }

    @Override
    public int length() {
        return stopOffset.offset-startOffset.offset;
    }

    @Override
    public char charAt(int index) {
        if(index + startOffset.offset >= stopOffset.offset || index < 0)
            throw new ArrayIndexOutOfBoundsException(
                    String.format("index is outside this span, span = [%d, %d), index = %d", startOffset.offset, stopOffset.offset, index+startOffset.offset));

        return parent.charAt(index + startOffset.offset);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        if(end + startOffset.offset > stopOffset.offset) {
            throw new ArrayIndexOutOfBoundsException(
                    String.format("end is outside this span, span = [%d, %d), requested = [%d, %d)",
                                  startOffset.offset, stopOffset.offset,
                                  start+startOffset.offset, end+startOffset.offset));
        }
        return parent.subSequence(start+startOffset.offset, end+startOffset.offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Span span = (Span) o;

        if (!parent.equals(span.parent)) return false;
        if (!startOffset.equals(span.startOffset)) return false;
        return stopOffset.equals(span.stopOffset);
    }

    @Override
    public int hashCode() {
        int result = parent.hashCode();
        result = 31 * result + startOffset.hashCode();
        result = 31 * result + stopOffset.hashCode();
        return result;
    }

    @Override
    public int compareTo(Span o) {
        int res = Integer.compare(startOffset.offset, o.startOffset.offset);
        return res == 0 ? Integer.compare(stopOffset.offset, o.stopOffset.offset) : res;
    }

    @Override
    public IntStream chars() {
        return parent.subSequence(startOffset.offset, stopOffset.offset).chars();
    }

    @Override
    public IntStream codePoints() {
        return parent.subSequence(startOffset.offset, stopOffset.offset).codePoints();
    }

    @Override
    public String toString() {
        return parent.subSequence(startOffset.offset, stopOffset.offset).toString();
    }
}
