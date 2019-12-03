package se.lth.cs.docria;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOError;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MsgpackDocument {
    private byte[] data;
    private Schema schema;

    private final static int PROPERTY_START = 0;
    private final static int PROPERTY_END = 1;

    private final static int SCHEMA_START = 2;
    private final static int SCHEMA_END = 3;

    private final static int TEXT_START = 4;
    private final static int TEXT_END = 5;

    private final static int LAYERS_START = 6;
    private final static int LAYERS_END = 7;

    private int[] positions = null;

    public MsgpackDocument(byte[] data) {
        this.data = data;
        //MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data);

        byte[] header = Arrays.copyOf(data, 4);
        if(!Arrays.equals(header, "DM_1".getBytes())) {
            throw new IOError(new UnsupportedOperationException("Unsupported document, header does not match expected values: " + new String(header, StandardCharsets.ISO_8859_1)));
        }
    }

    public MsgpackDocument(byte[] propertyCompartment, byte[] schemaCompartment, byte[] textCompartment, byte[] layerCompartment) {
        int totalSize = propertyCompartment.length + schemaCompartment.length + textCompartment.length + layerCompartment.length;
        this.data = new byte[totalSize];

        int pos = 0;
        System.arraycopy(propertyCompartment, 0, data, pos, propertyCompartment.length);
        pos += propertyCompartment.length;
        System.arraycopy(schemaCompartment, 0, data, pos, schemaCompartment.length);
        pos += schemaCompartment.length;
        System.arraycopy(textCompartment, 0, data, pos, textCompartment.length);
        pos += textCompartment.length;
        System.arraycopy(layerCompartment, 0, data, pos, layerCompartment.length);
    }

    public static Map<String,Value> parsePropertyCompartment(byte[] propertyCompartment) {
        try {
            return MsgpackCodec.decodeProperties(MessagePack.newDefaultUnpacker(propertyCompartment));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public static Schema parseSchemaCompartment(byte[] schemaCompartment, List<String> types, Map<String,List<String>> types2columns) {
        try {
            return MsgpackCodec.decodeSchema(MessagePack.newDefaultUnpacker(schemaCompartment), Collections.emptyMap(), types, types2columns);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public byte[] data() {
        return this.data;
    }

    private void determineCompartments() {
        if(this.positions == null) {
            try {
                MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data);
                unpacker.readPayloadAsReference(4);

                this.positions = new int[8]; // Start + End

                // 1. Find property compartment
                positions[PROPERTY_START] = (int)unpacker.getTotalReadBytes();
                int propLength = unpacker.unpackInt();
                unpacker.readPayloadAsReference(propLength);
                positions[PROPERTY_END] = (int)unpacker.getTotalReadBytes();

                List<String> types = new ArrayList<>();
                Map<String,List<String>> types2columns = new TreeMap<>();

                positions[SCHEMA_START] = (int)unpacker.getTotalReadBytes();

                // 2. Read Schema
                this.schema = MsgpackCodec.decodeSchema(unpacker, Collections.emptyMap(), types, types2columns);
                positions[SCHEMA_END] = (int)unpacker.getTotalReadBytes();

                // 3. Find text compartment
                positions[TEXT_START] = (int)unpacker.getTotalReadBytes();
                int sz = unpacker.unpackInt();
                unpacker.readPayloadAsReference(sz);
                positions[TEXT_END] = (int)unpacker.getTotalReadBytes();

                // 4. Find layer compartment
                positions[LAYERS_START] = (int)unpacker.getTotalReadBytes();
                int layer_sz = unpacker.unpackInt();
                unpacker.readPayloadAsReference(layer_sz);
                positions[LAYERS_END] = (int)unpacker.getTotalReadBytes();
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    }

    public Map<String, Value> properties() {
        determineCompartments();
        try {
            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data, positions[PROPERTY_START], positions[PROPERTY_END]);
            return MsgpackCodec.decodeProperties(unpacker);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public byte[] propertyCompartment() {
        determineCompartments();
        return Arrays.copyOfRange(data, positions[PROPERTY_START], positions[PROPERTY_END]);
    }

    public byte[] schemaCompartment() {
        determineCompartments();
        return Arrays.copyOfRange(data, positions[SCHEMA_START], positions[SCHEMA_END]);
    }

    public byte[] layersCompartment() {
        determineCompartments();
        return Arrays.copyOfRange(data, positions[LAYERS_START], positions[LAYERS_END]);
    }

    public byte[] textCompartment() {
        determineCompartments();
        return Arrays.copyOfRange(data, positions[TEXT_START], positions[TEXT_END]);
    }

    public Schema schema() {
        return schema;
    }

    public Document document() {
        return MsgpackCodec.decode(data);
    }
}
