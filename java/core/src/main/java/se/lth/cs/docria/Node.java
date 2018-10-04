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

import se.lth.cs.docria.values.NullValue;
import se.lth.cs.docria.values.Values;

import java.util.List;
import java.util.TreeMap;
import java.util.function.BiConsumer;

public class Node extends Value {
    protected TreeMap<String, Value> data;
    protected int id;
    protected Layer layer;

    /**
     * Internal use only.
     *
     * Use {@link Layer#create} instead.
     */
    protected Node() {
        this.data = new TreeMap<>();
    }

    public static class Builder {
        private final Layer layer;
        private TreeMap<String, Value> values = new TreeMap<>();
        private Schema.Layer schema;
        private Node instance;

        public Builder(Layer layer, Node instance) {
            this.layer = layer;
            this.schema = layer.schema();
            this.instance = instance;
        }

        public Builder put(String fieldName, Value value) {
            DataType fieldType = schema.getFieldType(fieldName);
            if(fieldType != null) {
                if(!value.type().equals(fieldType)) {
                    throw new IllegalArgumentException("value does not match expected type. Got: " + value.type().toString() + ", Expected: " + fieldType.toString());
                }
            }

            if(values.containsKey(fieldName)) {
                throw new IllegalArgumentException("Duplicate entry for " + fieldName);
            }

            values.put(fieldName, value);
            return this;
        }

        public Builder put(String fieldName, int value) {
            return put(fieldName, Values.get(value));
        }

        public Builder put(String fieldName, long value) {
            return put(fieldName, Values.get(value));
        }

        public Builder put(String fieldName, double value) {
            return put(fieldName, Values.get(value));
        }

        public Builder put(String fieldName, boolean value) {
            return put(fieldName, Values.get(value));
        }

        public Builder put(String fieldName, Node[] value) {
            return put(fieldName, Values.get(value));
        }

        public Builder put(String fieldName, List<Node> value) {
            return put(fieldName, Values.get(value));
        }

        public Builder put(String fieldName, String value) {
            return put(fieldName, Values.get(value));
        }

        public Builder put(String fieldName, byte[] value) {
            return put(fieldName, Values.get(value));
        }

        public Node insert() {
            instance.setData(values);
            layer.add(instance);
            return instance;
        }
    }

    protected void setData(TreeMap<String, Value> data) {
        this.data = data;
    }

    public static void setNodeData(Node node, TreeMap<String, Value> data) {
        node.setData(data);
    }

    public boolean isValid() {
        return layer != null;
    }

    public Layer layer() {
        return this.layer;
    }

    @Override
    public String stringValue() {
        return this.layer.name() + "#" + this.id;
    }

    @Override
    public Node nodeValue() {
        return this;
    }

    public void forEach(BiConsumer<String, Value> fieldFunction) {
        data.forEach(fieldFunction);
    }

    @Override
    public DataType type() {
        return layer.schema().layerType();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        return id == node.id && layer.equals(node.layer);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + layer.hashCode();
        return result;
    }

    @Override
    public String toString() {
        //TODO: Implement!
        return super.toString();
    }

    public Node put(String field, Value value) {
        data.put(field, value);
        return this;
    }

    public Node put(String field, int value) {
        return put(field, Values.get(value));
    }

    public Node put(String field, long value) {
        return put(field, Values.get(value));
    }

    public Node put(String field, double value) {

        return put(field, Values.get(value));
    }

    public Node put(String field, byte[] value) {
        return put(field, Values.get(value));
    }

    public Node put(String field, Node value) {
        return put(field, Values.get(value));
    }

    public Node put(String field, Node[] nodes) {
        return put(field, Values.get(nodes));
    }

    public Node put(String field, List<Node> nodes) {
        return put(field, Values.get(nodes));
    }

    public Node put(String field, String value) {
        return put(field, Values.get(value));
    }

    public Node put(String field, Span value) {
        return put(field, Values.get(value));
    }

    public Node put(String field, boolean value) {
        return put(field, Values.get(value));
    }

    public boolean has(String field) {
        return data.containsKey(field);
    }

    public void remove(String field) {
        data.remove(field);
    }

    public Value get(String field) {
        return data.getOrDefault(field, NullValue.INSTANCE);
    }

    public Value get(String field, Value defaultValue) {
        Value val = data.get(field);
        if(val == null)
            return defaultValue;
        else
            return val;
    }
}
