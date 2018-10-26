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

import java.util.TreeMap;

public enum DataTypeName {
    NULL("nil"),
    INT("i32"),
    LONG("i64"),
    DOUBLE("f64"),
    BOOL("i1"),
    STRING("str"),
    BINARY("bin"),
    SPAN("span"),
    EXT("ext"),
    NODE("noderef"),
    NODE_ARRAY("noderef_array");

    private final String name;

    private DataTypeName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    private static final TreeMap<String, DataTypeName> typeNames = new TreeMap<>();
    static {
        for (DataTypeName dataTypeName : DataTypeName.values()) {
            typeNames.put(dataTypeName.name, dataTypeName);
        }
    }

    public static DataTypeName from(String typeName) {
        return typeNames.get(typeName);
    }
}
