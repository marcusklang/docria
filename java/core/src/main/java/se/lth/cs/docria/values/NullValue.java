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

package se.lth.cs.docria.values;

import se.lth.cs.docria.*;

public class NullValue extends Value {
    private NullValue() {

    }

    public static NullValue INSTANCE = new NullValue();

    @Override
    public DataType type() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String stringValue() {
        return "";
    }

    @Override
    public boolean boolValue() {
        return false;
    }

    @Override
    public int intValue() {
        return 0;
    }

    @Override
    public long longValue() {
        return 0;
    }

    @Override
    public double doubleValue() {
        return 0.0;
    }

    @Override
    public byte[] binaryValue() {
        return new byte[0];
    }

    @Override
    public Node nodeValue() {
        throw new NullPointerException();
    }

    @Override
    public Node[] nodeArrayValue() {
        return new Node[0];
    }

    @Override
    public Span spanValue() {
        throw new NullPointerException();
    }

    @Override
    public NodeSpan nodeSpanValue() {
        throw new NullPointerException();
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }

    @Override
    public String toString() {
        return "Null";
    }
}
