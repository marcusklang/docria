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

public class BoolValue extends Value {
    private final boolean value;

    private BoolValue(boolean value) {
        this.value = value;
    }

    public static final BoolValue TRUE = new BoolValue(true);
    public static final BoolValue FALSE = new BoolValue(false);

    @Override
    public DataType type() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public String stringValue() {
        return String.valueOf(value);
    }

    @Override
    public int intValue() {
        return value ? 1 : 0;
    }

    @Override
    public long longValue() {
        return value ? 1L : 0L;
    }

    @Override
    public double doubleValue() {
        return value ? 1.0 : 0.0;
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }
}
