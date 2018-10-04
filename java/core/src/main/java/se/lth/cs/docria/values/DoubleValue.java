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

public class DoubleValue extends Value {
    private final double value;

    public double EPS = 1e-12;

    @Override
    public DataType type() {
        return DataTypes.FLOAT_64;
    }

    public DoubleValue(double value) {
        this.value = value;
    }

    @Override
    public String stringValue() {
        return null;
    }

    @Override
    public boolean boolValue() {
        return Math.abs(value) - EPS <= 0.0;
    }

    @Override
    public int intValue() {
        return (int)value;
    }

    @Override
    public long longValue() {
        return (long)value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }
}
