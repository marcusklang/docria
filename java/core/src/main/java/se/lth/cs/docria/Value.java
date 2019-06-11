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

public abstract class Value {

    public abstract String stringValue();

    public abstract DataType type();

    public boolean boolValue() {
        return Boolean.parseBoolean(stringValue());
    }

    public int intValue() {
        return Integer.parseInt(stringValue());
    }

    public long longValue() {
        return Long.parseLong(stringValue());
    }

    public double doubleValue() {
        return Double.parseDouble(stringValue());
    }

    public byte[] binaryValue() {
        throw new UnsupportedOperationException();
    }

    public Node nodeValue() {
        throw new UnsupportedOperationException();
    }

    public Node[] nodeArrayValue() {
        throw new UnsupportedOperationException();
    }

    public Span spanValue() {
        throw new UnsupportedOperationException();
    }

    public NodeSpan nodeSpanValue() { throw new UnsupportedOperationException(); }

    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }
}
