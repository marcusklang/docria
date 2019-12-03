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

import se.lth.cs.docria.exceptions.DataInconsistencyException;

import java.util.*;
import java.util.stream.Collectors;
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

    /**
     * Add layer
     *
     * @param layer layer to add
     * @return this document
     * @see Layer#create(String)
     * @see Document#add(Schema.Layer)
     */
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

    /**
     * Construct layer from schema
     *
     * @param schema schema configured
     * @return constructed layer
     * @see Layer#create(String) how to builder a layer schema
     */
    public Layer add(Schema.Layer schema) {
        Layer layer = new Layer(this, schema);
        this.schema.add(schema);
        this.layers.put(schema.name(), layer);
        return layer;
    }

    /**
     * Remove text, will fail if any layer is referring to this text.
     * @param text the text to remove, (null is a no-op)
     * @return this document instance
     */
    public Document remove(Text text) {
        return remove(text, false);
    }

    /**
     * Remove text
     * @param text         text to remove, (null is a no-op)
     * @param fieldcascade remove fields in layers referring to this text, cascading removal
     * @return this
     */
    public Document remove(Text text, boolean fieldcascade) {
        if(text == null)
            return this;

        //1. Verify no layer contains fields referencing this text.
        List<Layer> layersWithReferentFields =
                layers.values()
                      .stream()
                      .filter(l -> l.schema()
                                    .fields()
                                    .values()
                                    .stream()
                                    .anyMatch(dt -> dt.name() == DataTypeName.SPAN && dt.args().get("context").stringValue().equals(text.name()))
                      ).collect(Collectors.toList());

        if(!fieldcascade && !layersWithReferentFields.isEmpty()) {
            String context = layersWithReferentFields.stream()
                                                     .map(l -> l.name()
                                                             + "("
                                                             + l.schema()
                                                                .fields()
                                                                .entrySet()
                                                                .stream()
                                                                .filter(e -> e.getValue().name() == DataTypeName.SPAN
                                                                        && e.getValue().args()
                                                                            .get("context").stringValue()
                                                                            .equals(text.name()))
                                                                .map(Map.Entry::getKey)
                                                                .collect(Collectors.joining(", "))
                                                             + ")")
                                                     .collect(Collectors.joining(", "));

            throw new DataInconsistencyException(String.format("Removing text: %s results in data inconsistency, " +
                                                         "because layers+fields { %s } are referring to this text, " +
                                                         "remove these fields before removing text.", text.name(), context));
        }
        else if(fieldcascade) {
            for (Layer layersWithReferentField : layersWithReferentFields) {
                List<String> fields =
                        layersWithReferentField.schema()
                                               .fields()
                                               .entrySet()
                                               .stream()
                                               .filter(e -> e.getValue().name() == DataTypeName.SPAN && e.getValue().args().get("context").stringValue().equals(text.name()))
                                               .map(Map.Entry::getKey)
                                               .collect(Collectors.toList());

                fields.forEach(layersWithReferentField::removeField);
            }
        }

        return this;
    }

    /**
     * Remove layer, will fail if any layer is referring to the layer to be removed.
     * @param layer the layer to remove (null is a no-op)
     * @return this document instance
     */
    public Document remove(Layer layer) {
        return remove(layer, false);
    }

    /**
     * Remove layer
     * @param layer the layer to remove (null is a no-op)
     * @param fieldcascade remove referring fields from other layers, default: false
     *                     which will cast exception if any field of any layer is referring.
     * @return this document instance
     */
    public Document remove(Layer layer, boolean fieldcascade) {
        if(layer == null)
            return this;

        //1. Verify no layer contains fields referencing this text.
        List<Layer> layersWithReferentFields =
                layers.values()
                      .stream()
                      .filter(l -> l.schema()
                                    .fields()
                                    .values()
                                    .stream()
                                    .anyMatch(dt -> dt.name() == DataTypeName.SPAN && dt.args().get("context").stringValue().equals(layer.name()))
                      ).collect(Collectors.toList());

        if(!fieldcascade && !layersWithReferentFields.isEmpty()) {
            String context = layersWithReferentFields.stream()
                                                     .map(l -> l.name()
                                                             + "("
                                                             + l.schema()
                                                                .fields()
                                                                .entrySet()
                                                                .stream()
                                                                .filter(e -> (e.getValue().name() == DataTypeName.NODE
                                                                        || e.getValue().name() == DataTypeName.NODE_ARRAY)
                                                                        && e.getValue().args()
                                                                            .get("layer").stringValue()
                                                                            .equals(layer.name()))
                                                                .map(Map.Entry::getKey)
                                                                .collect(Collectors.joining(", "))
                                                             + ")")
                                                     .collect(Collectors.joining(", "));

            throw new DataInconsistencyException(String.format("Removing layer: %s results in data inconsistency, " +
                                                                       "because layers+fields { %s } are referring to this layer, " +
                                                                       "remove these fields before removing text.", layer.name(), context));
        }
        else if(fieldcascade) {
            for (Layer layersWithReferentField : layersWithReferentFields) {
                List<String> fields =
                        layersWithReferentField.schema()
                                               .fields()
                                               .entrySet()
                                               .stream()
                                               .filter(e -> (e.getValue().name() == DataTypeName.NODE
                                                       || e.getValue().name() == DataTypeName.NODE_ARRAY)
                                                       && e.getValue().args()
                                                           .get("layer").stringValue()
                                                           .equals(layer.name()))
                                               .map(Map.Entry::getKey)
                                               .collect(Collectors.toList());

                fields.forEach(layersWithReferentField::removeField);
            }
        }

        return this;
    }

    /**
     * @deprecated use {@link #add(Schema.Layer)} with {@link Layer#create(String)}
     *
     * @param name the name to define layer with
     * @return layer builder
     */
    public Schema.LayerBuilder layerBuilder(String name) {
        return Schema.layer(name);
    }

    public Text text(String name) {
        return texts.get(name);
    }

    /**
     * Convention method to access text("main")
     * @return main text
     */
    public Text maintext() {
        return text("main");
    }

    public Collection<Text> texts() {
        return texts.values();
    }

    public boolean hasText(String name) {
        return texts.containsKey(name);
    }

    public Stream<Text> textStream() {
        return texts.values().stream();
    }

    public Layer layer(String name) {
        return layers.get(name);
    }

    public boolean hasLayer(String name) {
        return layers.containsKey(name);
    }

    public static class Compiled {
        public final Map<String,List<String>> text2parts;

        public Compiled(Map<String, List<String>> text2parts) {
            this.text2parts = text2parts;
        }
    }

    /**
     * Validates schema by throwing an DataInconsitencyException if the schema is invalid.
     * @throws DataInconsistencyException thrown when schema is invalid.
     */
    public void validateSchema() {
        for (Layer value : this.layers.values()) {
            for (Map.Entry<String, DataType> field : value.schema().fields().entrySet()) {
                switch (field.getValue().name()) {
                    case SPAN:
                        if(!this.texts.containsKey(field.getValue().args().get("context").stringValue())) {
                            throw new DataInconsistencyException(
                                    String.format("Field '%s' in layer '%s' is referring " +
                                                          "to the text layer '%s' which cannot be found.",
                                                  field.getKey(),
                                                  value.name(),
                                                  field.getValue().args().get("context")));
                        }
                        break;
                    case NODE:
                    case NODE_ARRAY:
                        if(!this.layers.containsKey(field.getValue().args().get("layer").stringValue())) {
                            throw new DataInconsistencyException(
                                    String.format("Field '%s' in layer '%s' is referring " +
                                                          "to the node layer '%s' which cannot be found.",
                                                  field.getKey(),
                                                  value.name(),
                                                  field.getValue().args().get("layer")));
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }

    /**
     * For internal use: Compacts all layers and offsets in texts, used to prepare for serialization.
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
