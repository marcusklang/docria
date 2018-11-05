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

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class Schema {

    public static class Layer {
        private final String name;
        private final DataType typeNoderef;
        private final DataType typeNoderefArray;
        private final TreeMap<String,DataType> fields;
        private final NodeFactory factory;

        public Layer(String name, TreeMap<String, DataType> fields, NodeFactory factory) {
            this.name = name;
            this.fields = fields;
            this.typeNoderef = DataTypes.noderef(name);
            this.typeNoderefArray = DataTypes.noderef_array(name);
            this.factory = factory;
        }

        public NodeFactory factory() {
            return factory;
        }

        public DataType layerType() {
            return typeNoderef;
        }

        public DataType layerArrayType() {
            return typeNoderefArray;
        }

        public String name() {
            return name;
        }

        public Map<String,DataType> fields() {
            return Collections.unmodifiableMap(fields);
        }

        public void add(String name, DataType type) {
            if(fields.containsKey(name))
                throw new IllegalArgumentException("name");

            fields.put(name, type);
        }

        public void remove(String name) {
            fields.remove(name);
        }

        public DataType getFieldType(String field) {
            return fields.get(field);
        }
    }

    public static class LayerBuilder {
        private String name;
        private TreeMap<String,DataType> fields = new TreeMap<>();
        private NodeFactory factory = Node::new;

        public LayerBuilder(String name) {
            this.name = name;
        }

        public Schema.LayerBuilder addField(String name, DataType type) {
            if (fields.containsKey(name)) {
                throw new IllegalArgumentException("name '" + name + "' already exists.");
            }

            fields.put(name, type);
            return this;
        }

        public Schema.LayerBuilder setFactory(NodeFactory factory) {
            this.factory = factory;
            return this;
        }

        public Schema.Layer build() {
            return new Schema.Layer(name, fields, factory);
        }
    }

    public static LayerBuilder layer(String name) {
        return new LayerBuilder(name);
    }

    private TreeMap<String, Schema.Layer> layerSchema = new TreeMap<>();

    public Schema() {

    }

    public Schema(Schema schema) {
        this.layerSchema = new TreeMap<>(schema.layerSchema);
    }

    public void add(Schema.Layer layer) {
        layerSchema.put(layer.name(), layer);
    }

    public Stream<Schema.Layer> layers() {
        return layerSchema.values().stream();
    }
}
