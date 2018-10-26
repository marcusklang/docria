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

import se.lth.cs.docria.DataType;
import se.lth.cs.docria.DataTypes;
import se.lth.cs.docria.Value;

import java.util.Arrays;
import java.util.Base64;

public class BinaryValue extends Value {
    private final byte[] value;

    public BinaryValue(byte[] value) {
        this.value = value;
    }

    @Override
    public DataType type() {
        return DataTypes.BINARY;
    }

    @Override
    public String stringValue() {
        return Base64.getEncoder().encodeToString(value);
    }

    public byte[] binaryValue() {
        return value;
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        BinaryValue that = (BinaryValue) o;
        return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "Binary[len=" + value.length + "]";
    }
}
