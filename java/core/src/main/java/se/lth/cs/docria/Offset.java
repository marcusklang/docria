package se.lth.cs.docria;

public class Offset implements Comparable<Offset> {
    protected int id;
    protected int offset;
    protected int refcount;

    public Offset(int offset) {
        this.offset = offset;
    }

    @Override
    public int compareTo(Offset o) {
        return Integer.compare(offset, o.offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Offset offset1 = (Offset) o;

        if (id != offset1.id) return false;
        return offset == offset1.offset;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + offset;
        return result;
    }
}
