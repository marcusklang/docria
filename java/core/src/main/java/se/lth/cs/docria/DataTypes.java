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

public class DataTypes {
    public static final DataType NULL = new DataType.Builder(DataTypeName.NULL).build();
    public static final DataType BOOLEAN = new DataType.Builder(DataTypeName.BOOL).build();
    public static final DataType INT_32 = new DataType.Builder(DataTypeName.INT).build();
    public static final DataType INT_64 = new DataType.Builder(DataTypeName.LONG).build();
    public static final DataType FLOAT_64 = new DataType.Builder(DataTypeName.DOUBLE).build();
    public static final DataType STRING = new DataType.Builder(DataTypeName.STRING).build();
    public static final DataType BINARY = new DataType.Builder(DataTypeName.BINARY).build();

    public static DataType nodespan(String layer) {
        return new DataType.Builder(DataTypeName.NODE_SPAN).addArg("layer", layer).build();
    }

    public static DataType noderef(String layer) {
        return new DataType.Builder(DataTypeName.NODE).addArg("layer", layer).build();
    }

    public static DataType noderef_array(String layer) {
        return new DataType.Builder(DataTypeName.NODE_ARRAY).addArg("layer", layer).build();
    }

    public static DataType span(String context) {
        return new DataType.Builder(DataTypeName.SPAN).addArg("context", context).build();
    }

    public static DataType ext(String type) {
        return new DataType.Builder(DataTypeName.EXT).addArg("type", type).build();
    }
}
