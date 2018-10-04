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

package se.lth.cs.docria.io;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferOutput;
import se.lth.cs.docria.MsgpackCodec;

import java.io.ByteArrayInputStream;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.zip.*;

public class DeflateCodec {
    public static byte[] compress(byte[] data) {
        /*try {
            ByteOutputBuffer buffer = new ByteOutputBuffer(16*1024);
            GZIPOutputStream gzip = new GZIPOutputStream(buffer, 16*1024);
            MessagePacker packer = MessagePack.newDefaultPacker(gzip);
            packer.packInt(data.length);
            packer.addPayload(data);
            packer.close();
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new IOError(e);
        }*/

        Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
        deflater.setInput(data);
        deflater.finish();

        byte[] output = new byte[1024];

        ByteOutputBuffer outputBuffer = new ByteOutputBuffer(1024);

        do {
            int writtenBytes = deflater.deflate(output);
            outputBuffer.write(output, 0, writtenBytes);
        } while(!deflater.finished());

        deflater.end();
        return outputBuffer.toByteArray();
    }

    public static byte[] decompress(byte[] data) {
        try {
            /*GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(data), 16*1024);
            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(gzipInputStream);

            int numBytes = unpacker.unpackInt();
            byte[] output = new byte[numBytes];
            unpacker.readPayload(output);
            return output;*/

            ByteOutputBuffer outputBuffer = new ByteOutputBuffer(1024);

            Inflater inflater = new Inflater();
            inflater.setInput(data);

            byte[] buffer = new byte[1024];

            do {
                int written = inflater.inflate(buffer);
                outputBuffer.write(buffer, 0, written);
            } while(!inflater.finished());

            inflater.end();

            return outputBuffer.toByteArray();
        } catch (DataFormatException e) {
            throw new IOError(e);
        }
    }
}
