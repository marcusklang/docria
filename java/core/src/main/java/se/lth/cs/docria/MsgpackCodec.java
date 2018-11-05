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

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.value.ImmutableIntegerValue;
import org.msgpack.value.ImmutableValue;
import se.lth.cs.docria.values.*;

import java.io.IOError;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MsgpackCodec {
    private static class ValueWriter implements ValueVisitor {
        private MessagePacker packer;

        public ValueWriter(MessagePacker packer) {
            this.packer = packer;
        }

        @Override
        public void accept(Value value) {
            throw new UnsupportedOperationException("Encoding " + value.type().toString());
        }

        @Override
        public void accept(BoolValue value) {
            try {
                packer.packBoolean(value.boolValue());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(IntValue value) {
            try {
                packer.packInt(value.intValue());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(LongValue value) {
            try {
                packer.packLong(value.longValue());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(DoubleValue value) {
            try {
                packer.packDouble(value.doubleValue());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(NullValue value) {
            try {
                packer.packNil();
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(BinaryValue value) {
            try {
                byte[] bytes = value.binaryValue();
                packer.packBinaryHeader(bytes.length);
                packer.writePayload(bytes);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(StringValue value) {
            try {
                packer.packString(value.stringValue());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(Span value) {
            try {
                packer.packInt(value.startOffset().id);
                packer.packInt(value.stopOffset().id);
            }
            catch(IOException e) {
                throw new IOError(e);
            }
        }

        public boolean schemaless = false;

        @Override
        public void accept(ExtensionValue value) {
            try {
                byte[] bytes = value.binaryValue();
                if(schemaless) {
                    MessageBufferPacker typePacker = MessagePack.newDefaultBufferPacker();
                    typePacker.packString("doc");
                    typePacker.packBinaryHeader(bytes.length);
                    typePacker.addPayload(bytes);
                    byte[] data = typePacker.toByteArray();

                    packer.packExtensionTypeHeader((byte) 0, data.length);
                    packer.addPayload(data);
                } else {
                    packer.packBinaryHeader(bytes.length);
                    packer.addPayload(bytes);
                }
            }
            catch(IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(Node value) {
            try {
                packer.packInt(value.id);
            }
            catch(IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(NodeArrayValue value) {
            try {
                Node[] nodes = value.nodeArrayValue();
                packer.packArrayHeader(nodes.length);
                for (int i = 0; i < nodes.length; i++) {
                    packer.packInt(nodes[i].id);
                }
            } catch(IOException e) {
                throw new IOError(e);
            }
        }
    }

    private static class SchemaArgWriter implements ValueVisitor {
        private MessagePacker packer;

        public SchemaArgWriter(MessagePacker packer) {
            this.packer = packer;
        }

        @Override
        public void accept(Value value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void accept(BoolValue value) {
            try {
                packer.packBoolean(value.boolValue());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(IntValue value) {
            try {
                packer.packInt(value.intValue());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(LongValue value) {
            try {
                packer.packLong(value.longValue());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(DoubleValue value) {
            try {
                packer.packDouble(value.doubleValue());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(BinaryValue value) {
            try {
                byte[] bytes = value.binaryValue();
                packer.packBinaryHeader(bytes.length);
                packer.writePayload(bytes);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void accept(StringValue value) {
            try {
                packer.packString(value.stringValue());
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    }

    private static void encodeProperties(Document document, MessagePacker packer) throws IOException {
        MessageBufferPacker propPacker = MessagePack.newDefaultBufferPacker();

        Map<String, Value> props = document.props();
        propPacker.packMapHeader(props.size());

        ValueWriter propWriter = new ValueWriter(propPacker);
        propWriter.schemaless = true;
        props.forEach((key, value) -> {
            try {
                propPacker.packString(key);
                value.visit(propWriter);
            } catch (IOException ex) {
                throw new IOError(ex);
            }
        });

        packer.packInt((int)propPacker.getTotalWrittenBytes());
        packer.addPayload(propPacker.toByteArray());
    }

    private static void encodeSchema(Document document, MessagePacker packer, List<String> types, Map<String,List<String>> types2columns) {
        try {
            // Encode layers
            document.layerStream().map(Layer::name).forEach(types::add);

            packer.packArrayHeader(types.size());
            for (String type : types) {
                packer.packString(type);
            }

            SchemaArgWriter argWriter = new SchemaArgWriter(packer);

            // Encode fields
            for (String type : types) {
                Schema.Layer schema = document.layer(type).schema();
                List<String> fields = types2columns.computeIfAbsent(type, k -> new ArrayList<>());

                packer.packInt(schema.fields().size());
                for (Map.Entry<String, DataType> entry : schema.fields().entrySet()) {
                    packer.packString(entry.getKey());

                    DataType dtype = entry.getValue();
                    if(dtype.args().isEmpty()) {
                        packer.packBoolean(false);
                        packer.packString(dtype.name().getName());
                    } else {
                        packer.packBoolean(true);
                        packer.packString(dtype.name().getName());

                        Map<String, Value> args = dtype.args();
                        packer.packMapHeader(args.size());
                        for (Map.Entry<String, Value> argsEntry : args.entrySet()) {
                            packer.packString(argsEntry.getKey());
                            argsEntry.getValue().visit(argWriter);
                        }
                    }

                    fields.add(entry.getKey());
                }
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private static void encodeTexts(Document.Compiled compiled, Document document, MessagePacker packer) {
        MessageBufferPacker textPacker = MessagePack.newDefaultBufferPacker();

        try {
            textPacker.packMapHeader(document.texts().size());
            for (Text text : document.texts()) {
                textPacker.packString(text.name());
                List<String> parts = compiled.text2parts.get(text.name());

                textPacker.packArrayHeader(parts.size());
                for (String part : parts) {
                    textPacker.packString(part);
                }
            }

            packer.packInt((int)textPacker.getTotalWrittenBytes());
            packer.addPayload(textPacker.toByteArray());
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private static void encodeLayerData(MessagePacker packer, Layer layer, List<String> fields) {
        try {
            MessageBufferPacker layerPacker = MessagePack.newDefaultBufferPacker();

            Schema.Layer schema = layer.schema();
            ValueWriter writer = new ValueWriter(layerPacker);

            layerPacker.packInt(layer.size());

            for (String field : fields) {
                DataType dataType = schema.fields().get(field);
                layerPacker.packBoolean(false); //Future improvement of encoding!
                switch (dataType.name()) {
                    case SPAN:
                        layerPacker.packArrayHeader(layer.size()*2);
                        for (Node node : layer) {
                            Value spanValue = node.get(field);
                            if(spanValue instanceof NullValue) {
                                layerPacker.packNil();
                                layerPacker.packNil();
                            } else {
                                Span span = spanValue.spanValue();
                                layerPacker.packInt(span.startOffset().id);
                                layerPacker.packInt(span.stopOffset().id);
                            }
                        }
                        break;
                    default:
                        layerPacker.packArrayHeader(layer.size());
                        try {
                            for (Node node : layer) {
                                node.get(field).visit(writer);
                            }
                        } catch (RuntimeException e) {
                            throw new RuntimeException(String.format("When processing column %s in layer %s", field, layer.name()), e);
                        }
                        break;
                }
            }

            packer.packInt((int)layerPacker.getTotalWrittenBytes());
            packer.addPayload(layerPacker.toByteArray());
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public static MessageBuffer encode(Document document) {
        try {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            Document.Compiled compiled = document.compile();

            //1. Write header
            packer.writePayload("DM_1".getBytes());

            //2. Write properties
            encodeProperties(document, packer);

            List<String> types = new ArrayList<>();
            Map<String,List<String>> types2columns = new TreeMap<>();

            //3. Write schema
            encodeSchema(document, packer, types, types2columns);

            //4. Write texts
            encodeTexts(compiled, document, packer);

            //5. Write Layer data
            for (String type : types) {
                encodeLayerData(packer, document.layer(type), types2columns.get(type));
            }

            return packer.toMessageBuffer();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public static Document decode(byte[] data) {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data);
        return decode(unpacker, null);
    }

    public static Document decode(byte[] data, Map<String,NodeFactory> nodeFactories) {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data);
        return decode(unpacker, nodeFactories);
    }

    protected static Map<String,Value> decodeProperties(MessageUnpacker unpacker) throws IOException {
        int compartmentSize = unpacker.unpackInt();

        TreeMap<String, Value> props = new TreeMap<>();

        int numEntries = unpacker.unpackMapHeader();
        for(int k = 0; k < numEntries; k++) {
            String key = unpacker.unpackString();
            ImmutableValue value = unpacker.unpackValue();
            switch (value.getValueType()) {
                case STRING:
                    props.put(key, Values.get(value.asStringValue().toString()));
                    break;
                case BOOLEAN:
                    props.put(key, Values.get(value.asBooleanValue().getBoolean()));
                    break;
                case INTEGER:
                    ImmutableIntegerValue ivalue = value.asIntegerValue();
                    if(ivalue.isInIntRange())
                        props.put(key, Values.get(ivalue.toInt()));
                    else
                        props.put(key, Values.get(ivalue.toLong()));

                    break;
                case FLOAT:
                    props.put(key, Values.get(value.asFloatValue().toDouble()));
                    break;
                case BINARY:
                    props.put(key, Values.get(value.asBinaryValue().asByteArray()));
                    break;
                case EXTENSION:
                    if(value.asExtensionValue().getType() == 0) {
                        MessageUnpacker extensionData = MessagePack.newDefaultUnpacker(value.asExtensionValue().getData());
                        String type = extensionData.unpackString();
                        switch (type) {
                            case "doc":
                                props.put(key, new DocumentValue(extensionData.unpackValue().asBinaryValue().asByteArray()));
                                break;
                            default:
                                props.put(key, new ExtensionValue(type, extensionData.unpackValue().asBinaryValue().asByteArray()));
                                break;
                        }
                    }
                    else
                        throw new UnsupportedOperationException(
                                "Unsupported extension type: " + value.asExtensionValue().getType() + " for property key: " + key);
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported property type: " + value.getValueType().toString());
            }
        }

        return props;
    }

    protected static Schema decodeSchema(
            MessageUnpacker unpacker,
            Map<String,NodeFactory> nodeFactories,
            List<String> types,
            Map<String,List<String>> types2fields
    ) throws IOException {
        Schema schema = new Schema();

        int numLayers = unpacker.unpackArrayHeader();
        for(int i = 0; i < numLayers; i++)
            types.add(unpacker.unpackString());

        for (String layerName : types) {
            List<String> fields = new ArrayList<>();

            int numFields = unpacker.unpackInt();

            Schema.LayerBuilder layerBuilder = Schema.layer(layerName);

            for(int i = 0; i < numFields; i++) {
                String fieldName = unpacker.unpackString();
                boolean extendedType = unpacker.unpackBoolean();
                DataType dtype;
                String typeName = unpacker.unpackString();

                if(extendedType) {
                    TreeMap<String,Value> args = new TreeMap<>();

                    int numEntries = unpacker.unpackMapHeader();
                    for(int k = 0; k < numEntries; k++) {
                        String key = unpacker.unpackString();
                        ImmutableValue value = unpacker.unpackValue();
                        switch (value.getValueType()) {
                            case STRING:
                                args.put(key, Values.get(value.asStringValue().toString()));
                                break;
                            case BOOLEAN:
                                args.put(key, Values.get(value.asBooleanValue().getBoolean()));
                                break;
                            case INTEGER:
                                ImmutableIntegerValue ivalue = value.asIntegerValue();
                                if(ivalue.isInIntRange())
                                    args.put(key, Values.get(ivalue.toInt()));
                                else
                                    args.put(key, Values.get(ivalue.toLong()));

                                break;
                            case FLOAT:
                                args.put(key, Values.get(value.asFloatValue().toFloat()));
                                break;
                            case BINARY:
                                args.put(key, Values.get(value.asBinaryValue().asByteArray()));
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported layer type argument: " + value.getValueType().toString());
                        }
                    }

                    dtype = new DataType.Builder(DataTypeName.from(typeName), args).build();
                } else {
                    switch (DataTypeName.from(typeName)) {
                        case BINARY:
                            dtype = DataTypes.BINARY;
                            break;
                        case STRING:
                            dtype = DataTypes.STRING;
                            break;
                        case BOOL:
                            dtype = DataTypes.BOOLEAN;
                            break;
                        case DOUBLE:
                            dtype = DataTypes.FLOAT_64;
                            break;
                        case INT:
                            dtype = DataTypes.INT_32;
                            break;
                        case LONG:
                            dtype = DataTypes.INT_64;
                            break;
                        default:
                            throw new UnsupportedOperationException("Unsupported layer type: " + fieldName);
                    }
                }

                fields.add(fieldName);
                layerBuilder.addField(fieldName, dtype);
            }

            types2fields.put(layerName, fields);
            if(nodeFactories != null && nodeFactories.containsKey(layerName)) {
                layerBuilder.setFactory(nodeFactories.get(layerName));
            }

            schema.add(layerBuilder.build());
        }

        return schema;
    }

    protected static Map<String, List<Offset>> decodeTexts(Document doc, MessageUnpacker unpacker) throws IOException {
        TreeMap<String,List<Offset>> text2offsets = new TreeMap<>();
        int sz = unpacker.unpackInt();

        int numTexts = unpacker.unpackMapHeader();
        for(int i = 0; i < numTexts; i++) {
            String context = unpacker.unpackString();
            int numParts = unpacker.unpackArrayHeader();
            StringBuilder sb = new StringBuilder();

            int offset = 0;
            List<Offset> offsets = new ArrayList<>(numParts);
            offsets.add(new Offset(0));

            for(int k = 0; k < numParts; k++) {
                String part = unpacker.unpackString();
                sb.append(part);
                offset = sb.length();
                offsets.add(new Offset(offset));
            }

            Text text = new Text(context, sb.toString());
            text.initializeOffsets(offsets);

            doc.add(text);

            text2offsets.put(context, offsets);
        }

        return text2offsets;
    }

    public static class NodeArrayPlaceholder extends Value {
        private int[] nodeIds;

        public NodeArrayPlaceholder(int[] nodeIds) {
            this.nodeIds = nodeIds;
        }

        @Override
        public String stringValue() {
            return null;
        }

        @Override
        public DataType type() {
            throw new UnsupportedOperationException();
        }
    }

    protected static void decodeLayer(Document document, Layer layer, List<String> fields, Map<String, List<Offset>> offsets, MessageUnpacker unpacker) throws IOException {
        int layer_sz = unpacker.unpackInt();
        int numNodes = unpacker.unpackInt();

        Schema.Layer schema = layer.schema();
        ArrayList<Node> nodes = new ArrayList<>(numNodes);
        for (int i = 0; i < numNodes; i++) {
            nodes.add(layer.create().insert());
        }

        for (String field : fields) {
            DataType fieldType = schema.getFieldType(field);
            boolean specialized = unpacker.unpackBoolean();
            if(specialized)
                throw new UnsupportedOperationException("Specialized encoding not yet implemented.");

            int numElements = unpacker.unpackArrayHeader();

            switch (fieldType.name()) {
                case INT:
                    for (int k = 0; k < numNodes; k++) {
                        if(!unpacker.tryUnpackNil()) {
                            nodes.get(k).put(field, unpacker.unpackInt());
                        }
                    }
                    break;
                case DOUBLE:
                    for (int k = 0; k < numNodes; k++) {
                        if(!unpacker.tryUnpackNil()) {
                            nodes.get(k).put(field, unpacker.unpackDouble());
                        }
                    }
                    break;
                case STRING:
                    for (int k = 0; k < numNodes; k++) {
                        if(!unpacker.tryUnpackNil()) {
                            nodes.get(k).put(field, unpacker.unpackString());
                        }
                    }
                    break;
                case LONG:
                    for (int k = 0; k < numNodes; k++) {
                        if(!unpacker.tryUnpackNil()) {
                            nodes.get(k).put(field, unpacker.unpackLong());
                        }
                    }
                    break;
                case BOOL:
                    for (int k = 0; k < numNodes; k++) {
                        if(!unpacker.tryUnpackNil()) {
                            nodes.get(k).put(field, unpacker.unpackBoolean());
                        }
                    }
                    break;
                case BINARY:
                    for (int k = 0; k < numNodes; k++) {
                        if(!unpacker.tryUnpackNil()) {
                            int numBytes = unpacker.unpackArrayHeader();
                            nodes.get(k).put(field, unpacker.readPayload(numBytes));
                        }
                    }
                    break;
                case SPAN:
                    String context = fieldType.args().get("context").stringValue();
                    List<Offset> contextOffsets = offsets.get(context);
                    Text txt = document.text(context);

                    for (int k = 0; k < numNodes; k++) {
                        if(!unpacker.tryUnpackNil()) {
                            int startOffset = unpacker.unpackInt();
                            int stopOffset = unpacker.unpackInt();

                            nodes.get(k).put(field,
                                             txt.unsafeSpanFromOffset(
                                                     contextOffsets.get(startOffset)
                                                   , contextOffsets.get(stopOffset)));
                        } else {
                            unpacker.unpackNil();
                        }
                    }
                    break;
                case EXT:
                    String type = fieldType.args().get("type").stringValue();
                    if(type.equals("doc")) {
                        for (int k = 0; k < numNodes; k++) {
                            if(!unpacker.tryUnpackNil()) {
                                nodes.get(k).put(field, new DocumentValue(unpacker.unpackValue().asBinaryValue().asByteArray()));
                            }
                        }
                    } else {
                        for (int k = 0; k < numNodes; k++) {
                            if(!unpacker.tryUnpackNil()) {
                                nodes.get(k).put(field, new ExtensionValue(type, unpacker.unpackValue().asBinaryValue().asByteArray()));
                            }
                        }
                    }
                    break;
                case NODE:
                    for (int k = 0; k < numNodes; k++) {
                        if(!unpacker.tryUnpackNil()) {
                            nodes.get(k).put(field, unpacker.unpackInt());
                        }
                    }
                    break;
                case NODE_ARRAY:
                    for (int k = 0; k < numNodes; k++) {
                        if(!unpacker.tryUnpackNil()) {
                            int numNodeEntries = unpacker.unpackArrayHeader();
                            int[] nodeIds = new int[numNodeEntries];
                            for (int j = 0; j < numNodeEntries; j++) {
                                nodeIds[j] = unpacker.unpackInt();
                            }

                            nodes.get(k).put(field, new NodeArrayPlaceholder(nodeIds));
                        }
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Field type not yet implemented: " + fieldType.name().getName());
            }
        }
    }

    protected static void resolveReferences(Document document, Layer layer) {
        Schema.Layer schema = layer.schema();
        for (Map.Entry<String, DataType> entry : schema.fields().entrySet()) {
            String field = entry.getKey();
            if(entry.getValue().name() == DataTypeName.NODE) {
                String targetLayerName = entry.getValue().args().get("layer").stringValue();
                Layer targetLayer = document.layer(targetLayerName);

                layer.forEach(n -> {
                    if(n.has(field)) {
                        int nodeId = n.get(field).intValue();
                        n.put(field, targetLayer.get(nodeId));
                    }
                });
            } else if(entry.getValue().name() == DataTypeName.NODE_ARRAY) {
                String targetLayerName = entry.getValue().args().get("layer").stringValue();
                Layer targetLayer = document.layer(targetLayerName);

                layer.forEach(n -> {
                    if(n.has(field)) {
                        int[] nodeIds = ((NodeArrayPlaceholder)n.get(field)).nodeIds;
                        Node[] nodes = new Node[nodeIds.length];
                        for (int k = 0; k < nodeIds.length; k++) {
                            nodes[k] = targetLayer.get(nodeIds[k]);
                        }

                        n.put(field, nodes);
                    }
                });
            }
        }
    }

    public static Document decode(MessageUnpacker unpacker, Map<String,NodeFactory> nodeFactories) {
        try {
            byte[] header = unpacker.readPayload(4);
            if(Arrays.equals(header, "DM_1".getBytes())) {
                // 1. Decode properties
                Map<String,Value> values = decodeProperties(unpacker);

                List<String> types = new ArrayList<>();
                Map<String,List<String>> types2columns = new TreeMap<>();

                // 2. Decode Schema
                Schema schema = decodeSchema(unpacker, nodeFactories, types, types2columns);

                Document doc = new Document(schema);
                doc.props().putAll(values);

                // 3. Decode texts
                Map<String, List<Offset>> offsets = decodeTexts(doc, unpacker);

                // 4. Decode layer data
                for (String type : types) {
                    decodeLayer(doc, doc.layer(type), types2columns.get(type), offsets, unpacker);
                }

                // 5. Resolve node references and replace placeholders with real node objects.
                for (String type : types)
                    resolveReferences(doc, doc.layer(type));

                return doc;
            }
            else {
                throw new IOError(new UnsupportedOperationException("Unsupported document, header does not match expected values: " + new String(header, StandardCharsets.ISO_8859_1)));
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
