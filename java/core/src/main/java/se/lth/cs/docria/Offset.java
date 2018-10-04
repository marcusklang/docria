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
