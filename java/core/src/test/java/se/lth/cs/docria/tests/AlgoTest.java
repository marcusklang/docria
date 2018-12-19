package se.lth.cs.docria.tests;

import org.junit.Assert;
import org.junit.Test;
import se.lth.cs.docria.*;
import se.lth.cs.docria.algorithms.GroupBySpan;

import java.util.*;
import java.util.stream.Collectors;


public class AlgoTest {

    private void verifyTokenSet(int id, Map<Integer, GroupBySpan.Group> id2group, int...set) {
        GroupBySpan.Group group = id2group.get(id);
        Set<Integer> actual;
        if(set.length == 0) {
            actual = new HashSet<>();
        } else {
            actual = group.content.get("tokens").stream().map(n -> n.get("id").intValue()).collect(Collectors.toSet());
        }

        Set<Integer> expected = Arrays.stream(set).boxed().distinct().collect(Collectors.toSet());
        Assert.assertEquals("Anchor with id " + id + " does not contain expected set", expected, actual);
    }

    @Test
    public void testGroupBySpanIntersect() {
        Document doc = new Document();
        Text raw_text = doc.add(new Text("raw", "0123456789012345678901234567890"));

        Layer tokens = doc.add(Layer.create("token")
                                    .addField("id", DataTypes.INT_32)
                                    .addField("span", raw_text.spanType())
                                    .build());

        Layer anchors = doc.add(Layer.create("anchor")
                                    .addField("id", DataTypes.INT_32)
                                    .addField("mention", raw_text.spanType())
                                    .build());

        Layer entities = doc.add(Layer.create("entity")
                                     .addField("id", DataTypes.INT_32)
                                     .addField("entity", raw_text.spanType())
                                     .build());

        tokens.create().put("id", 1).put("span", raw_text.span(0,2)).insert();
        tokens.create().put("id", 2).put("span", raw_text.span(3,5)).insert();
        tokens.create().put("id", 3).put("span", raw_text.span(4,6)).insert();
        tokens.create().put("id", 4).put("span", raw_text.span(8,11)).insert();
        tokens.create().put("id", 5).put("span", raw_text.span(11,12)).insert();
        tokens.create().put("id", 6).put("span", raw_text.span(14,15)).insert();
        tokens.create().put("id", 7).put("span", raw_text.span(17,17)).insert();

        anchors.create().put("id", 8).put("mention", raw_text.span(0,6)).insert();
        anchors.create().put("id", 9).put("mention", raw_text.span(1,6)).insert();
        anchors.create().put("id", 10).put("mention", raw_text.span(2,6)).insert();
        anchors.create().put("id", 11).put("mention", raw_text.span(4,10)).insert();
        anchors.create().put("id", 12).put("mention", raw_text.span(4,12)).insert();
        anchors.create().put("id", 13).put("mention", raw_text.span(8,11)).insert();
        anchors.create().put("id", 14).put("mention", raw_text.span(16,18)).insert();
        anchors.create().put("id", 15).put("mention", raw_text.span(20,20)).insert();

        entities.create().put("id", 16).put("entity", raw_text.span(4,6)).insert();
        entities.create().put("id", 17).put("entity", raw_text.span(8,12)).insert();
        entities.create().put("id", 18).put("entity", raw_text.span(0,15)).insert();

        List<GroupBySpan.Group> groups = GroupBySpan.builder(anchors, "mention")
                                                    .group("tokens", tokens, "span")
                                                    .group("entity", entities, "entity")
                                                    .solve();

        Map<Integer, GroupBySpan.Group> id2group = new TreeMap<>();

        for (GroupBySpan.Group group : groups) {
            id2group.put(group.node.get("id").intValue(), group);
        }

        for(int i = 8; i < 15; i++)
            Assert.assertNotNull("anchor with id " + i + " is null", id2group.get(i));

        verifyTokenSet(8, id2group, 1,2,3);
        verifyTokenSet(9, id2group, 1,2,3);
        verifyTokenSet(10, id2group, 2,3);
        verifyTokenSet(11, id2group, 2,3,4);
        verifyTokenSet(12, id2group, 2,3,4,5);
        verifyTokenSet(13, id2group, 4);
        verifyTokenSet(14, id2group, 7);
        verifyTokenSet(15, id2group);
    }

    @Test
    public void testGroupBySpanCover() {
        Document doc = new Document();
        Text raw_text = doc.add(new Text("raw", "0123456789012345678901234567890"));

        Layer tokens = doc.add(Layer.create("token")
                                    .addField("id", DataTypes.INT_32)
                                    .addField("span", raw_text.spanType())
                                    .build());

        Layer anchors = doc.add(Layer.create("anchor")
                                     .addField("id", DataTypes.INT_32)
                                     .addField("mention", raw_text.spanType())
                                     .build());

        Layer entities = doc.add(Layer.create("entity")
                                      .addField("id", DataTypes.INT_32)
                                      .addField("entity", raw_text.spanType())
                                      .build());

        tokens.create().put("id", 1).put("span", raw_text.span(0,2)).insert();
        tokens.create().put("id", 2).put("span", raw_text.span(3,5)).insert();
        tokens.create().put("id", 3).put("span", raw_text.span(4,6)).insert();
        tokens.create().put("id", 4).put("span", raw_text.span(8,11)).insert();
        tokens.create().put("id", 5).put("span", raw_text.span(11,12)).insert();
        tokens.create().put("id", 6).put("span", raw_text.span(14,15)).insert();
        tokens.create().put("id", 7).put("span", raw_text.span(17,17)).insert();

        anchors.create().put("id", 8).put("mention", raw_text.span(0,6)).insert();
        anchors.create().put("id", 9).put("mention", raw_text.span(1,6)).insert();
        anchors.create().put("id", 10).put("mention", raw_text.span(2,6)).insert();
        anchors.create().put("id", 11).put("mention", raw_text.span(4,10)).insert();
        anchors.create().put("id", 12).put("mention", raw_text.span(4,12)).insert();
        anchors.create().put("id", 13).put("mention", raw_text.span(8,11)).insert();
        anchors.create().put("id", 14).put("mention", raw_text.span(16,18)).insert();
        anchors.create().put("id", 15).put("mention", raw_text.span(20,20)).insert();

        entities.create().put("id", 16).put("entity", raw_text.span(4,6)).insert();
        entities.create().put("id", 17).put("entity", raw_text.span(8,12)).insert();
        entities.create().put("id", 18).put("entity", raw_text.span(0,15)).insert();

        List<GroupBySpan.Group> groups = GroupBySpan.builder(anchors, "mention")
                                                    .group("tokens", tokens, "span")
                                                    .group("entity", entities, "entity")
                                                    .resolution(GroupBySpan.Resolution.COVER)
                                                    .includeEmptyGroups(false)
                                                    .solve();

        Map<Integer, GroupBySpan.Group> id2group = new TreeMap<>();

        for (GroupBySpan.Group group : groups) {
            id2group.put(group.node.get("id").intValue(), group);
        }

        for(int i = 8; i < 14; i++)
            Assert.assertNotNull("anchor with id " + i + " is null", id2group.get(i));

        verifyTokenSet(8, id2group, 1,2,3);
        verifyTokenSet(9, id2group, 2,3);
        verifyTokenSet(10, id2group, 2,3);
        verifyTokenSet(11, id2group, 3);
        verifyTokenSet(12, id2group, 3,4,5);
        verifyTokenSet(13, id2group, 4);
        verifyTokenSet(14, id2group, 7);
        Assert.assertNull(id2group.get(15));
    }
}
