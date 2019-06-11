/**
 * Copyright 2018 Marcus Klang (marcus.klang@cs.lth.se)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package se.lth.cs.docria;

import se.lth.cs.docria.values.ValueVisitor;

import java.util.stream.IntStream;

public class Span extends Value implements CharSequence, Comparable<Span> {
    private Text source;
    private Offset startOffset;
    private Offset stopOffset;

    public Text source() {
        return source;
    }

    public Span(Text source, Offset startOffset, Offset stopOffset) {
        this.source = source;
        this.startOffset = startOffset;
        this.stopOffset = stopOffset;
    }

    protected void incref() {
        startOffset.refcount += 1;
        stopOffset.refcount += 1;
    }

    @Override
    public DataType type() {
        return source.spanType();
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

    public boolean intersects(Span span) {
        return  this.start() <= span.stop() && this.stop() >= span.start() ;
    }

    public boolean covers(Span span) {
        return span.start() >= this.start() && span.stop() <= this.stop();
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

        return source.charAt(index + startOffset.offset);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        if(end + startOffset.offset > stopOffset.offset) {
            throw new ArrayIndexOutOfBoundsException(
                    String.format("end is outside this span, span = [%d, %d), requested = [%d, %d)",
                                  startOffset.offset, stopOffset.offset,
                                  start+startOffset.offset, end+startOffset.offset));
        }
        return source.subSequence(start+startOffset.offset, end+startOffset.offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Span span = (Span) o;

        if (!source.equals(span.source)) return false;
        if (!startOffset.equals(span.startOffset)) return false;
        return stopOffset.equals(span.stopOffset);
    }

    @Override
    public int hashCode() {
        int result = source.hashCode();
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
        return source.subSequence(startOffset.offset, stopOffset.offset).chars();
    }

    @Override
    public IntStream codePoints() {
        return source.subSequence(startOffset.offset, stopOffset.offset).codePoints();
    }

    @Override
    public String toString() {
        return source.subSequence(startOffset.offset, stopOffset.offset).toString();
    }
}
