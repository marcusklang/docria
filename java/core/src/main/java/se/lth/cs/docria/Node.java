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
import java.util.Map;
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

    public int id() {
        return this.id;
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
            else {
                throw new IllegalArgumentException(String.format("field %s does not exist in schema.", fieldName));
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

        public Node build() {
            if(!values.isEmpty())
                instance.setData(values);

            return instance;
        }

        /**
         * @deprecated use {@link Layer#add(Node)} in combination with {@link Layer#create()}
         * @return
         */
        @Deprecated
        public Node insert() {
            if(!values.isEmpty())
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
        StringBuilder sb = new StringBuilder();
        sb.append("Node[").append(layer != null ? layer.name() : "???").append("](");

        boolean first = true;
        for (Map.Entry<String, Value> stringValueEntry : data.entrySet()) {
            if(first) {
                first = false;
            } else {
                sb.append(", ");
            }
            if(stringValueEntry.getValue().type().name() == DataTypeName.NODE) {
                Node n = stringValueEntry.getValue().nodeValue();
                sb.append(stringValueEntry.getKey()).append(" = ").append(n.layer().name()).append("#").append(n.id);
            }
            else {
                sb.append(stringValueEntry.getKey()).append(" = ").append(stringValueEntry.getValue());
            }

        }
        sb.append(")");
        return sb.toString();
    }

    public Node put(String field, Value value) {
        data.put(field, value);
        return this;
    }

    /**
     * Validating builder for a node
     * @param layer the layer to validate against
     * @return builder
     */
    public static Builder builder(Layer layer) {
        return new Node.Builder(layer, layer.nodeFactory.create());
    }

    public final Node put(final String field, int value) {
        return put(field, Values.get(value));
    }

    public final Node put(final String field, long value) {
        return put(field, Values.get(value));
    }

    public final Node put(final String field, double value) {
        return put(field, Values.get(value));
    }

    public final Node put(final String field, byte[] value) {
        return put(field, Values.get(value));
    }

    public final Node put(final String field, Node value) {
        return put(field, Values.get(value));
    }

    public final Node put(final String field, Node[] nodes) {
        return put(field, Values.get(nodes));
    }

    public final Node put(final String field, List<Node> nodes) {
        return put(field, Values.get(nodes));
    }

    public final Node put(final String field, String value) {
        return put(field, Values.get(value));
    }

    public final Node put(final String field, Span value) {
        return put(field, Values.get(value));
    }

    public final Node put(final String field, boolean value) {
        return put(field, Values.get(value));
    }

    public boolean has(final String field) {
        return data.containsKey(field);
    }

    public void remove(final String field) {
        data.remove(field);
    }

    public Value get(final String field) {
        return data.getOrDefault(field, NullValue.INSTANCE);
    }

    public final String getString(final String field) {
        return get(field).stringValue();
    }

    public final int getInt(final String field) {
        return get(field).intValue();
    }

    public final long getLong(final String field) {
        return get(field).longValue();
    }

    public final double getDouble(final String field) {
        return get(field).doubleValue();
    }

    public final boolean getBool(final String field) {
        return get(field).boolValue();
    }

    public Value get(String field, Value defaultValue) {
        Value val = data.get(field);
        if(val == null)
            return defaultValue;
        else
            return val;
    }
}
