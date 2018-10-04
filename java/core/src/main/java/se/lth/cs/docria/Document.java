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

import java.util.*;
import java.util.stream.Stream;

public class Document {
    private final Schema schema;

    private TreeMap<String, Layer> layers = new TreeMap<>();
    private TreeMap<String, Text> texts = new TreeMap<>();
    private HashMap<String, Value> props = new HashMap<>();

    public Document() {
        this.schema = new Schema();
    }

    public Document(Schema schema) {
        this.schema = new Schema(schema);
        schema.layers()
              .forEach(layerSchema -> layers.put(layerSchema.name(), new Layer(this, layerSchema)));
    }

    public Schema schema() {
        return this.schema;
    }

    public Map<String, Value> props() {
        return props;
    }

    public Iterable<Layer> layers() {
        return layers.values();
    }

    public Stream<Layer> layerStream() {
        return layers.values().stream();
    }

    public Document add(Layer layer) {
        if(layers.containsKey(layer.name())) {
            throw new IllegalArgumentException("Document already contains a layer with name '" + layer.name() + "'");
        }

        layers.put(layer.name(), layer);
        return this;
    }

    public Text add(Text text) {
        if(texts.containsKey(text.name()))
            throw new IllegalArgumentException("Duplicate entry: " + text.name());

        texts.put(text.name(), text);
        return text;
    }

    public Layer add(Schema.Layer schema) {
        Layer layer = new Layer(this, schema);
        this.schema.add(schema);
        this.layers.put(schema.name(), layer);
        return layer;
    }

    public Schema.LayerBuilder layerBuilder(String name) {
        return Schema.layer(name);
    }

    public Text text(String name) {
        return texts.get(name);
    }

    public Collection<Text> texts() {
        return texts.values();
    }

    public Stream<Text> textStream() {
        return texts.values().stream();
    }

    public Layer layer(String name) {
        return layers.get(name);
    }

    public static class Compiled {
        public final Map<String,List<String>> text2parts;

        public Compiled(Map<String, List<String>> text2parts) {
            this.text2parts = text2parts;
        }
    }

    /**
     * Compacts all layers and offsets in texts, used to prepare for serialization
     */
    public Compiled compile() {
        layerStream().forEach(Layer::compact);
        texts.forEach( (k,v) -> v.reset());

        layerStream().forEach(layer -> {
            List<String> spanFields = new ArrayList<>();
            List<String> spanContexts = new ArrayList<>();

            layer.schema().fields().entrySet().stream().filter(e -> e.getValue().name().equals(DataTypeName.SPAN)).forEach(e -> {
                spanFields.add(e.getKey());
                spanContexts.add((String)e.getValue().args().get("context").stringValue());
            });

            if(spanFields.size() > 0) {
                layer.forEach(n -> {
                    for (String spanField : spanFields) {
                        if(n.has(spanField))
                            n.get(spanField).spanValue().incref();
                    }
                });
            }
        });

        TreeMap<String,List<String>> text2parts = new TreeMap<>();
        texts.forEach( (k,v) -> text2parts.put(k, v.compile()));

        return new Compiled(text2parts);
    }

}
