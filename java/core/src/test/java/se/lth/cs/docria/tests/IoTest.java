package se.lth.cs.docria.tests;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.Assert;
import org.junit.Test;
import se.lth.cs.docria.*;
import se.lth.cs.docria.io.*;
import se.lth.cs.docria.values.Values;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class IoTest {

    private byte[] generateData() {
        //Write exactly 4096*2 bytes
        byte[] data = new byte[4096*2];
        for(int i = 0, k = 1; i < (4096*2); i++) {
            data[i] = (byte)k++;
            if(k == 128)
                k = 1;
        }
        return data;
    }

    @Test
    public void testComputeLength() {
        Assert.assertEquals(4095, BoundaryReader.computeLength(12, 1, 4096));
        Assert.assertEquals(4095, BoundaryReader.computeLength(12, 1, 4096+4));
        Assert.assertEquals(4096, BoundaryReader.computeLength(12, 1, 4096+4+1));


        Assert.assertEquals(4092, BoundaryReader.computeLength(12, 4096, 8192));
        Assert.assertEquals(4092, BoundaryReader.computeLength(12, 4096, 8192+4));
        Assert.assertEquals(4096, BoundaryReader.computeLength(12, 4096, 8192+4+4));

        Assert.assertEquals(8192, BoundaryReader.computeLength(12, 1, 8201));
        Assert.assertEquals(4092, BoundaryReader.computeLength(12, 4100, 8192));
        Assert.assertEquals(4092, BoundaryReader.computeLength(12, 4100, 8196));

        Assert.assertEquals(4096, BoundaryReader.computeLength(12, 4100, 8200));
    }

    @Test
    public void testSimple() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BoundaryWriter writer = new BoundaryWriter(baos, 12);
            writer.split();

            byte[] data = generateData();
            //Boundary tests
            writer.write(data, 0, 4096);
            writer.split();

            writer.write(data, 4096, 4096);
            writer.flush();

            byte[] written = baos.toByteArray();
            Assert.assertEquals(8192+1+8, written.length);

            byte[] delta1 = Arrays.copyOfRange(written, 4096, 4100);
            byte[] delta2 = Arrays.copyOfRange(written, 8192, 8196);

            ByteArrayInputStream bais = new ByteArrayInputStream(delta1);
            DataInputStream dos = new DataInputStream(bais);
            int idelta1 = dos.readInt();

            Assert.assertEquals(-4095, idelta1);

            ByteArrayInputStream bais2 = new ByteArrayInputStream(delta2);
            DataInputStream dos2 = new DataInputStream(bais2);
            int idelta2 = dos2.readInt();
            Assert.assertEquals(-4091, idelta2);

            ByteArrayInputStream boundaryReader = new ByteArrayInputStream(written);
            BoundaryReader reader = new BoundaryReader(boundaryReader);

            byte[] read = new byte[8192];
            Assert.assertEquals(8192, reader.read(read));

            Assert.assertArrayEquals(data, read);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Test
    public void testDocumentStream() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DocumentStreamBoundaryWriter streamWriter = new DocumentStreamBoundaryWriter(12, output, 16, DocumentStreamWriter.Codec.DEFLATE_SQUARED);

        long k = 0;

        for(int i = 0; i < 128; i++) {
            List<String> somenumbers = LongStream.range(k, k + 128).mapToObj(String::valueOf).collect(Collectors.toList());
            k += 128;

            Document doc = new Document();
            doc.props().put("uri", Values.get(String.format("doc:%d", i)));

            IntArrayList offsets = new IntArrayList(128);
            StringBuilder sb = new StringBuilder();
            for (String somenumber : somenumbers) {
                offsets.add(sb.length());
                sb.append(somenumber);
                offsets.add(sb.length());
                sb.append(" ");
            }

            Text main = doc.add(new Text("main", sb.toString()));

            Layer tokenlayer = doc.add(Schema.layer("token").addField("text", main.spanType()).build());
            for(int j = 0; j < 128; j++) {
                tokenlayer.create().put("text", main.span(offsets.getInt(j*2), offsets.getInt(j*2+1))).insert();
            }

            streamWriter.write(doc);
        }

        streamWriter.flush();
        streamWriter.close();

        byte[] rawData = output.toByteArray();

        ByteArrayInputStream input = new ByteArrayInputStream(rawData);
        DocumentStreamReader reader = new DocumentStreamReader(new BoundaryReader(input));

        k = 0;
        int j = 0;
        Document doc;
        while((doc = reader.next()) != null) {
            Assert.assertEquals(String.format("doc:%d", j), doc.props().get("uri").stringValue());

            List<String> somenumbers = LongStream.range(k, k + 128).mapToObj(String::valueOf).collect(Collectors.toList());

            String saved = doc.layer("token").stream().map(n -> n.get("text")).map(Value::stringValue).collect(Collectors.joining(" "));
            String expected = String.join(" ", somenumbers);

            Assert.assertEquals(expected, saved);

            k += 128;
            j++;
        }

        Assert.assertTrue(reader.next() == null);
    }

    @Test
    public void testDocumentStreamLargeBlock() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DocumentStreamBoundaryWriter streamWriter = new DocumentStreamBoundaryWriter(12, output, 1024, DocumentStreamWriter.Codec.DEFLATE_SQUARED);

        long k = 0;

        for(int i = 0; i < 2048; i++) {
            List<String> somenumbers = LongStream.range(k, k + 128).mapToObj(String::valueOf).collect(Collectors.toList());
            k += 128;

            Document doc = new Document();
            doc.props().put("uri", Values.get(String.format("doc:%d", i)));

            IntArrayList offsets = new IntArrayList(128);
            StringBuilder sb = new StringBuilder();
            for (String somenumber : somenumbers) {
                offsets.add(sb.length());
                sb.append(somenumber);
                offsets.add(sb.length());
                sb.append(" ");
            }

            Text main = doc.add(new Text("main", sb.toString()));

            Layer tokenlayer = doc.add(Schema.layer("token").addField("text", main.spanType()).build());
            for(int j = 0; j < 128; j++) {
                tokenlayer.create().put("text", main.span(offsets.getInt(j*2), offsets.getInt(j*2+1))).insert();
            }

            streamWriter.write(doc);
        }

        streamWriter.flush();
        streamWriter.close();

        byte[] rawData = output.toByteArray();

        ByteArrayInputStream input = new ByteArrayInputStream(rawData);
        DocumentStreamReader reader = new DocumentStreamReader(new BoundaryReader(input));

        k = 0;
        int j = 0;
        Document doc;
        while((doc = reader.next()) != null) {
            Assert.assertEquals(String.format("doc:%d", j), doc.props().get("uri").stringValue());

            List<String> somenumbers = LongStream.range(k, k + 128).mapToObj(String::valueOf).collect(Collectors.toList());

            String saved = doc.layer("token").stream().map(n -> n.get("text")).map(Value::stringValue).collect(Collectors.joining(" "));
            String expected = somenumbers.stream().collect(Collectors.joining(" "));

            Assert.assertEquals(expected, saved);

            k += 128;
            j++;
        }

        Assert.assertNull(reader.next());
    }
}
