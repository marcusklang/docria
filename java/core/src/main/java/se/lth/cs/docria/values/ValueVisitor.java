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

import se.lth.cs.docria.Node;
import se.lth.cs.docria.Span;
import se.lth.cs.docria.Value;

public interface ValueVisitor {
    public void accept(Value value);

    public default void accept(BoolValue value) {
        this.accept((Value)value);
    }

    public default void accept(IntValue value) {
        this.accept((Value)value);
    }

    public default void accept(LongValue value) {
        this.accept((Value)value);
    }

    public default void accept(DoubleValue value) {
        this.accept((Value)value);
    }

    public default void accept(NullValue value) {
        this.accept((Value)value);
    }

    public default void accept(BinaryValue value) {
        this.accept((Value)value);
    }

    public default void accept(StringValue value) {
        this.accept((Value)value);
    }

    public default void accept(Span value) {
        this.accept((Value)value);
    }

    public default void accept(Node value) {
        this.accept((Value)value);
    }

    public default void accept(NodeArrayValue value) {
        this.accept((Value)value);
    }
}
