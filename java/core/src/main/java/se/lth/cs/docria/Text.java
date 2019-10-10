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

import it.unimi.dsi.fastutil.ints.*;
import se.lth.cs.docria.exceptions.SpanException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Text implements CharSequence {
    private final String name;
    private final Int2ObjectOpenHashMap<Offset> pos2Offset = new Int2ObjectOpenHashMap<>();
    private final DataType spantype;
    private String text;
    private boolean validating=true;

    public boolean isValidating() {
        return validating;
    }

    public void setValidating(boolean validating) {
        this.validating = validating;
    }

    public DataType spanType() {
        return spantype;
    }

    public Text(String text) {
        this("main", text);
    }

    public Text(String name, String text) {
        this.name = name;
        this.text = text;
        this.spantype = DataTypes.span(this.name);
    }

    public static void reset(Text node) {
        node.reset();
    }

    public static void compile(Text node) {
        node.compile();
    }

    public void initializeOffsets(List<Offset> offsets) {
        offsets.forEach(off -> pos2Offset.put(off.offset, off));
    }

    /**
     * Resets ref counts
     */
    protected void reset() {
        pos2Offset.values().forEach(offset -> offset.refcount = 0);
    }

    public void setText(String text) {
        this.text = text;
    }

    /**
     * Based on ref counts, compacts and assigns id to offsets
     */
    protected List<String> compile() {
        int id = 0;

        {
            IntArrayList removal = new IntArrayList();
            pos2Offset.values().stream().filter(off -> off.refcount == 0).mapToInt(off -> off.offset).forEach(removal::add);
            IntListIterator iter = removal.iterator();
            while (iter.hasNext()) {
                pos2Offset.remove(iter.nextInt());
            }
        }

        IntAVLTreeSet sortedSet = new IntAVLTreeSet(pos2Offset.keySet());

        if(!sortedSet.contains(0)){
            Offset start = new Offset(0);
            pos2Offset.put(0, start);
            sortedSet.add(0);
        }

        if(!sortedSet.contains(text.length())) {
            Offset stop = new Offset(text.length());
            pos2Offset.put(text.length(), stop);
            sortedSet.add(text.length());
        }

        List<String> positions = new ArrayList<>();

        if(sortedSet.size() == 1) {
            //Occurs when string is of length 0
            positions.add("");
        }
        else
        {
            IntBidirectionalIterator iter = sortedSet.iterator();

            int startOffset = iter.nextInt();
            pos2Offset.get(startOffset).id = id++;
            do {
                int stopOffset = iter.nextInt();
                pos2Offset.get(stopOffset).id = id++;

                positions.add(text.substring(startOffset, stopOffset));
                startOffset = stopOffset;
            } while (iter.hasNext());
        }

        return positions;
    }

    public String name() {
        return name;
    }

    public Span span(int startOffset, int stopOffset) {
        if(validating && !(
                (startOffset >= 0 && startOffset < text.length())
             && (stopOffset >= 0 && stopOffset <= text.length())
             && (stopOffset >= startOffset)
        )) {
            throw new SpanException(String.format("Span with start=%d, stop=%d is not acceptable. Text length=%d", startOffset, stopOffset, text.length()));
        }

        Offset start, stop;
        if(pos2Offset.containsKey(startOffset)) {
            start = pos2Offset.get(startOffset);
        } else {
            start = new Offset(startOffset);
            pos2Offset.put(startOffset, start);
        }

        if(pos2Offset.containsKey(stopOffset)) {
            stop = pos2Offset.get(stopOffset);
        } else {
            stop = new Offset(stopOffset);
            pos2Offset.put(stopOffset, stop);
        }

        return new Span(this, start, stop);
    }

    /**
     * Unsafe internal method for creating spans.
     *
     * Used in deserialization when unique offsets are known.
     * @param startOffset start offset (must match internal offset)
     * @param stopOffset stop offset (must match internal offset)
     * @return
     */
    public Span unsafeSpanFromOffset(Offset startOffset, Offset stopOffset) {
        return new Span(this, startOffset, stopOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Text text1 = (Text) o;

        if (!name.equals(text1.name)) return false;
        return text.equals(text1.text);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + text.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return text;
    }

    @Override
    public int length() {
        return text.length();
    }

    @Override
    public char charAt(int index) {
        return text.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return text.subSequence(start, end);
    }

    @Override
    public IntStream chars() {
        return text.chars();
    }

    @Override
    public IntStream codePoints() {
        return text.codePoints();
    }
}
