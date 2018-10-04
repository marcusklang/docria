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

import se.lth.cs.docria.values.Values;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class DataType {
    public static class Builder {
        private DataTypeName name;
        private TreeMap<String, Value> args;

        public Builder(DataTypeName name) {
            if(name == null)
                throw new NullPointerException("name");

            this.name = name;
            this.args = new TreeMap<>();
        }

        public Builder(DataTypeName name, TreeMap<String, Value> args) {
            this.name = name;
            this.args = args;
        }

        private Builder addArg(String key, Value value) {
            if(key == null)
                throw new NullPointerException("key");

            if(value == null)
                throw new NullPointerException("value");

            if(args.containsKey(key)) {
                throw new IllegalArgumentException("Duplicate argument: " + key);
            }

            args.put(key, value);
            return this;
        }

        public Builder addArg(String key, boolean value) {
            return this.addArg(key, Values.get(value));
        }

        public Builder addArg(String key, int value) {
            return this.addArg(key, Values.get(value));
        }

        public Builder addArg(String key, long value) {
            return this.addArg(key, Values.get(value));
        }

        public Builder addArg(String key, String value) {
            return this.addArg(key, Values.get(value));
        }

        public Builder addArg(String key, byte[] value) {
            return this.addArg(key, Values.get(value));
        }

        public Builder addArg(String key, double value) {
            return this.addArg(key, Values.get(value));
        }

        public DataType build() {
            return new DataType(this.name, this.args);
        }
    }

    private DataTypeName name;
    private TreeMap<String, Value> args;

    private DataType(DataTypeName name, TreeMap<String, Value> args) {
        this.name = name;
        this.args = args;
    }

    public DataTypeName name() {
        return this.name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataType dataType = (DataType) o;

        if (!name.equals(dataType.name)) return false;
        return args.equals(dataType.args);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + args.hashCode();
        return result;
    }

    public Map<String, Value> args() {
        return Collections.unmodifiableMap(args);
    }

    @Override
    public String toString() {
        if(args.size() > 0)
            return String.format("Type[%s, %s]", name, args.entrySet().stream().map(e -> String.format("%s=%s", e.getKey(), e.getValue())).collect(Collectors.joining(", ")));
        else
            return String.format("Type[%s]", name);
    }
}
