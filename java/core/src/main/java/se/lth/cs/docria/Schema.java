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
            this.typeNoderef = new DataType.Builder(DataTypeName.NODE).addArg("layer", name).build();
            this.typeNoderefArray = new DataType.Builder(DataTypeName.NODE_ARRAY).addArg("layer", name).build();
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
