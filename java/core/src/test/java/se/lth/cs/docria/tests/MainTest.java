package se.lth.cs.docria.tests;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import se.lth.cs.docria.*;
import se.lth.cs.docria.io.DeflateCodec;
import se.lth.cs.docria.values.Values;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MainTest {

    private Pattern pattern = Pattern.compile("\\p{L}+|\\p{N}+|[^\\s]", Pattern.UNICODE_CHARACTER_CLASS);

    public static class Token extends ConcreteNode {
        private Span text;

        protected Token() {

        }

        public Token(Span text) {
            this.text = text;
        }

        public static Schema.Layer schema(String name, String text) {
            return Schema.layer(name)
                         .addField("text", DataTypes.span(text))
                         .setFactory(Token::new)
                         .build();
        }

        private final static ConcreteNode.NodeInterface nodeInterface =
                new ConcreteNode.Builder<Token>()
                        .addField("text", n -> n.text, (n,v) -> n.text = v.spanValue())
                        .build();

        @Override
        protected final NodeInterface nodeInterface() {
            return nodeInterface;
        }

        public Span getText() {
            return text;
        }

        public Token setText(Span text) {
            this.text = text;
            return this;
        }
    }

    public static class NamedEntity extends ConcreteNode {
        private Span text;
        private String cls;

        protected NamedEntity() {

        }

        public NamedEntity(Span text, String cls) {
            this.text = text;
            this.cls = cls;
        }

        public static Schema.Layer schema(String name, String text) {
            return Schema.layer(name)
                         .addField("text", DataTypes.span(text))
                         .addField("cls", DataTypes.STRING)
                         .setFactory(NamedEntity::new)
                         .build();
        }

        private final static ConcreteNode.NodeInterface nodeInterface =
                new ConcreteNode.Builder<NamedEntity>()
                        .addField("text", n -> n.text, (n,v) -> n.text = v.spanValue())
                        .addField("cls", n -> Values.get(n.cls), (n,v) -> n.cls = v.stringValue())
                        .build();

        @Override
        protected NodeInterface nodeInterface() {
            return nodeInterface;
        }

        public Span getText() {
            return text;
        }

        public void setText(Span text) {
            this.text = text;
        }

        public String getCls() {
            return cls;
        }

        public void setCls(String cls) {
            this.cls = cls;
        }
    }


    @Test
    public void testSpecific()
    {
        Document doc = new Document();
        Text main_text = doc.add(new Text("main", "This code was written in Lund, Sweden."));
        //                                         01234567890123456789012345678901234567
        //                                         0         1         2         3

        Layer tokenLayer = doc.add(Token.schema("token", "main"));
        Layer neLayer = doc.add(NamedEntity.schema("named_entity", "main"));

        Matcher matcher = pattern.matcher(main_text);
        while(matcher.find()) {
            tokenLayer.add(new Token(main_text.span(matcher.start(), matcher.end())));
        }

        neLayer.add(new NamedEntity(main_text.span(25, 29), "GPE"));
        neLayer.add(new NamedEntity(main_text.span(31, 37), "GPE"));

        TreeMap<String, NodeFactory> factoryTreeMap = new TreeMap<>();
        factoryTreeMap.put("token", Token::new);
        factoryTreeMap.put("named_entity", NamedEntity::new);

        doc = MsgpackCodec.decode(MsgpackCodec.encode(doc).toByteArray(), factoryTreeMap);
        tokenLayer = doc.layer("token");
        neLayer = doc.layer("named_entity");

        String tokenText = tokenLayer.stream(Token.class).map(Token::getText).map(Span::toString).collect(Collectors.joining(" "));
        assertEquals("This code was written in Lund , Sweden .", tokenText);

        String neText = neLayer.stream(NamedEntity.class).map(NamedEntity::getText).map(Span::toString).collect(Collectors.joining(" "));
        assertEquals("Lund Sweden", neText);
    }

    @Test
    public void testPython() {
        String base64 = "RE1fMQGAkqV0b2tlbqxuYW1lZF9lbnRpdHkBpHRleHTDpHNwYW6Bp2NvbnRleHSkbWFpbgKkdGV4dMOkc3BhboGnY29" +
                "udGV4dKRtYWluo2Nsc8Kjc3RyPIGkbWFpbp+kVGhpc6EgpGNvZGWhIKN3YXOhIKd3cml0dGVuoSCiaW6hIKRMdW5koSyhIKZTd2" +
                "VkZW6hLhcJwtwAEgABAgMEBQYHCAkKCwsMDQ4ODxECwpQKCw0OwpKjR1BFo0dQRQ==";

        Document doc = MsgpackCodec.decode(Base64.getDecoder().decode(base64));
        Layer tokenLayer = doc.layer("token");
        Layer neLayer = doc.layer("named_entity");

        String tokenText = tokenLayer.stream().map(n -> n.get("text").spanValue().toString()).collect(Collectors.joining(" "));
        assertEquals("This code was written in Lund , Sweden .", tokenText);

        String neText = neLayer.stream().map(n -> n.get("text").spanValue().toString()).collect(Collectors.joining(" "));
        assertEquals("Lund Sweden", neText);
    }

    @Test
    public void testGzipCodec() {
        StringBuilder redundant = new StringBuilder();
        for(int i = 0; i < 1000; i++) {
            redundant.append("AB");
        }

        // Test Redundant
        byte[] rawdata = redundant.toString().getBytes(StandardCharsets.UTF_8);

        byte[] compress = DeflateCodec.compress(rawdata);
        //System.out.println(String.format("Compress length: %d, Raw: %d", compress.length, rawdata.length));

        byte[] decompressed = DeflateCodec.decompress(compress);
        assertArrayEquals(rawdata, decompressed);

        //Test High Entropy
        rawdata = new byte[1024*1024];
        Random rand = new Random(1234567L);
        rand.nextBytes(rawdata);

        compress = DeflateCodec.compress(rawdata);
        //System.out.println(String.format("Compress length: %d, Raw: %d", compress.length, rawdata.length));

        decompressed = DeflateCodec.decompress(compress);

        assertArrayEquals(rawdata, decompressed);
    }

    @Test
    public void testGeneric() {
        Document doc = new Document();
        Text main_text = doc.add(new Text("main", "This code was written in Lund, Sweden."));
        //                                         01234567890123456789012345678901234567
        //                                         0         1         2         3

        Layer tokenLayer = doc.add(Schema.layer("token")
                                         .addField("text", main_text.spanType())
                                         .build());

        Layer neLayer = doc.add(Schema.layer("named_entity")
                                      .addField("text", main_text.spanType())
                                      .addField("cls", DataTypes.STRING).build());

        Matcher matcher = pattern.matcher(main_text);
        while(matcher.find()) {
            tokenLayer.create()
                      .put("text", main_text.span(matcher.start(), matcher.end()))
                      .insert();
        }

        neLayer.create()
               .put("text", main_text.span(25, 29))
               .put("cls", "GPE")
               .insert();

        neLayer.create()
               .put("text", main_text.span(31, 37))
               .put("cls", "GPE")
               .insert();

        doc = MsgpackCodec.decode(MsgpackCodec.encode(doc).toByteArray());
        tokenLayer = doc.layer("token");
        neLayer = doc.layer("named_entity");

        String tokenText = tokenLayer.stream().map(n -> n.get("text").spanValue().toString()).collect(Collectors.joining(" "));
        assertEquals("This code was written in Lund , Sweden .", tokenText);

        String neText = neLayer.stream().map(n -> n.get("text").spanValue().toString()).collect(Collectors.joining(" "));
        assertEquals("Lund Sweden", neText);
    }


}
