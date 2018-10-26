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

import java.util.Objects;

public class StringValue extends Value {
    private final String value;

    public StringValue(String value) {
        this.value = value;
    }

    @Override
    public DataType type() {
        return DataTypes.STRING;
    }

    @Override
    public String stringValue() {
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
        StringValue that = (StringValue) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "String[" + value + "]";
    }
}
