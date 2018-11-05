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
    private int propertyStart;
    private int propertyLength;
    private Schema schema;

    public MsgpackDocument(byte[] data) {
        this.data = data;
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data);

        try {
            byte[] header = unpacker.readPayload(4);
            if(Arrays.equals(header, "DM_1".getBytes())) {
                // 1. Decode properties
                this.propertyStart = (int)unpacker.getTotalReadBytes();
                this.propertyLength = unpacker.unpackInt();
                int compartmentsz = ((int)unpacker.getTotalReadBytes() - this.propertyStart);
                unpacker.readPayloadAsReference(this.propertyLength);
                this.propertyLength += compartmentsz;

                //Map<String,Value> values = MsgpackCodec.decodeProperties(unpacker);

                List<String> types = new ArrayList<>();
                Map<String,List<String>> types2columns = new TreeMap<>();

                // 2. Decode Schema
                this.schema = MsgpackCodec.decodeSchema(unpacker, Collections.emptyMap(), types, types2columns);

            }
            else {
                throw new IOError(new UnsupportedOperationException("Unsupported document, header does not match expected values: " + new String(header, StandardCharsets.ISO_8859_1)));
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public Map<String, Value> properties() {
        try {
            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data, propertyStart, propertyLength);
            return MsgpackCodec.decodeProperties(unpacker);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public Schema schema() {
        return schema;
    }

    public Document document() {
        return MsgpackCodec.decode(data);
    }
}
