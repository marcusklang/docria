package se.lth.cs.docria.algorithms;

import se.lth.cs.docria.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GroupBySpan {
    public enum Resolution {
        INTERSECT,
        COVER
    }

    public static class Group {
        public final Node node;
        public final Map<String,List<Node>> content;

        public Group(Node node, Map<String, List<Node>> content) {
            this.node = node;
            this.content = content;
        }

        public boolean empty() {
            return content.values().stream().allMatch(List::isEmpty);
        }
    }

    public static Builder builder(List<Node> groups) {
        return builder(groups, "text");
    }

    public static Builder builder(List<Node> groups, String spanfield) {
        return new Builder(groups, "text");
    }

    public static Builder builder(Layer groups) {
        return builder(groups, "text");
    }

    public static Builder builder(Document doc, String grouplayer) {
        return builder(doc.layer(grouplayer), "text");
    }

    public static Builder builder(Document doc, String grouplayer, String spanfield) {
        return builder(doc.layer(grouplayer), spanfield);
    }

    public static Builder builder(Layer groups, String spanfield) {
        DataType fieldType = groups.schema().getFieldType(spanfield);
        if(fieldType == null) {
            throw new IllegalArgumentException("Spanfield: " + spanfield + " does not exist on layer: " + groups.name());
        }
        else if(fieldType.name() != DataTypeName.SPAN) {
            throw new IllegalArgumentException("Spanfield: " + spanfield + " is not a span on layer: " + groups.name());
        }

        return new Builder(new ArrayList<>(groups), spanfield);
    }

    private static class Point implements Comparable<Point> {
        private int type;
        private int offset;
        private String layer;
        private Node node;

        public Point(int type, int offset, String layer, Node node) {
            this.type = type;
            this.offset = offset;
            this.layer = layer;
            this.node = node;
        }

        @Override
        public int compareTo(Point o) {
            int res = Integer.compare(offset, o.offset);

            if(res == 0) {
                res = Integer.compare(type, o.type);
            }

            if(res == 0) {
                if(layer == null && o.layer != null)
                    res = -1;
                else if(layer != null && o.layer == null)
                    res = 1;
                else if(layer != null)
                    res = layer.compareTo(o.layer);
            }

            return res;
        }
    }

    public static class Builder {
        private List<Node> groups;
        private String groupSpanField;

        private Map<String,List<Node>> layers = new TreeMap<>();
        private Map<String,String> layerSpanfield = new TreeMap<>();
        private Resolution resolution = Resolution.INTERSECT;
        private boolean includeEmptyGroups;

        public Builder(List<Node> groups, String groupSpanField) {
            this.groups = groups;
            this.groupSpanField = groupSpanField;
        }

        public Builder group(String name, List<Node> nodes) {
            return group(name, nodes, "text");
        }

        public Builder group(String name, List<Node> nodes, String spanfield) {
            layers.put(name, nodes);
            layerSpanfield.put(name, spanfield);
            return this;
        }

        public Builder group(Document doc, String layer) {
            return this.group(layer, doc.layer(layer));
        }

        public Builder group(Document doc, String layer, String spanfield) {
            return this.group(layer, doc.layer(layer), spanfield);
        }

        public Builder group(Layer layer) {
            return this.group(layer.name(), layer);
        }

        public Builder group(Layer layer, String spanfield) {
            return this.group(layer.name(), layer, spanfield);
        }

        public Builder group(String name, Layer layer) {
            return group(name, layer, "text");
        }

        public Builder group(String name, Layer layer, String spanfield) {
            DataType fieldType = layer.schema().getFieldType(spanfield);
            if(fieldType == null) {
                throw new IllegalArgumentException("Spanfield: " + spanfield + " does not exist on layer: " + layer.name());
            }
            else if(fieldType.name() != DataTypeName.SPAN) {
                throw new IllegalArgumentException("Spanfield: " + spanfield + " is not a span on layer: " + layer.name());
            }

            return group(name, new ArrayList<>(layer), spanfield);
        }

        public Builder resolution(Resolution resolution) {
            this.resolution = resolution;
            return this;
        }

        public Builder includeEmptyGroups(boolean includeEmptyGroups) {
            this.includeEmptyGroups = includeEmptyGroups;
            return this;
        }

        private void addGroups(List<Point> points) {
            groups.forEach(n -> {
                if(n.has(groupSpanField)) {
                    Span span = n.get(groupSpanField).spanValue();
                    if(span.start() != span.stop()) {
                        Point start = new Point(0, span.start(), null, n);
                        Point stop = new Point(-2, span.stop(), null, n);
                        points.add(start);
                        points.add(stop);
                    }
                    else {
                        Point start = new Point(3, span.start(), null, n);
                        Point stop = new Point(5, span.stop(), null, n);
                        points.add(start);
                        points.add(stop);
                    }
                }
            });
        }

        private void addLayers(List<Point> points) {
            layers.forEach( (layer, nodes) -> {
                String spanfield = layerSpanfield.get(layer);
                nodes.forEach(n -> {
                    if(n.has(spanfield)) {
                        Span span = n.get(spanfield).spanValue();
                        if(span.start() != span.stop()) {
                            Point start = new Point(1, span.start(), layer, n);
                            Point stop = new Point(-1, span.stop(), layer, n);
                            points.add(start);
                            points.add(stop);
                        }
                        else {
                            Point start = new Point(4, span.start(), layer, n);
                            Point stop = new Point(6, span.stop(), layer, n);
                            points.add(start);
                            points.add(stop);
                        }
                    }
                });
            });
        }

        private Map<String,List<Node>> createLayerGroups() {
            TreeMap<String,List<Node>> layer2nodes = new TreeMap<>();
            layers.keySet().forEach(key -> layer2nodes.put(key, new ArrayList<>()));
            return layer2nodes;
        }

        private static class LayerNode {
            private String layer;
            private Node node;

            public LayerNode(String layer, Node node) {
                this.layer = layer;
                this.node = node;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
                LayerNode layerNode = (LayerNode) o;
                return Objects.equals(layer, layerNode.layer) && Objects.equals(node, layerNode.node);
            }

            @Override
            public int hashCode() {
                return Objects.hash(layer, node);
            }
        }

        private List<Group> sweep(List<Point> points) {
            List<Group> groups = new ArrayList<>();
            Map<String, Set<Node>> open_nodes = new TreeMap<>();
            HashSet<Node> open_groups = new HashSet<>();
            HashMap<Node,Group> node2group = new HashMap<>();

            layers.keySet().forEach(key -> open_nodes.put(key, new HashSet<>()));

            for (Point point : points) {
                switch (point.type) {
                    // Group start
                    case 0:
                    case 3:
                        open_groups.add(point.node);
                        Group group = new Group(point.node, createLayerGroups());
                        groups.add(group);
                        node2group.put(point.node, group);

                        if(resolution == Resolution.COVER) {
                            Span parentSpan = point.node.get(groupSpanField).spanValue();

                            for (Map.Entry<String, Set<Node>> entry : open_nodes.entrySet()) {
                                String layer = entry.getKey();
                                String spanfield = layerSpanfield.get(layer);
                                List<Node> nodes = group.content.get(layer);

                                for (Node node : entry.getValue()) {
                                    Span span = node.get(spanfield).spanValue();
                                    if(span.start() >= parentSpan.start() && span.stop() <= parentSpan.stop()) {
                                        nodes.add(node);
                                    }
                                }
                            }
                        }
                        else {
                            for (Map.Entry<String, Set<Node>> entry : open_nodes.entrySet()) {
                                String layer = entry.getKey();
                                List<Node> nodes = group.content.get(layer);
                                nodes.addAll(entry.getValue());
                            }
                        }

                        break;
                    // Layer Start
                    case 1:
                    case 4:
                        open_nodes.get(point.layer).add(point.node);
                        if(resolution == Resolution.COVER) {
                            for (Node open_group : open_groups) {
                                Span parentSpan = open_group.get(groupSpanField).spanValue();
                                Span span = point.node.get(layerSpanfield.get(point.layer)).spanValue();

                                if(span.start() >= parentSpan.start() && span.stop() <= parentSpan.stop()) {
                                    node2group.get(open_group).content.get(point.layer).add(point.node);
                                }
                            }
                        } else {
                            for (Node open_group : open_groups) {
                                node2group.get(open_group).content.get(point.layer).add(point.node);
                            }
                        }
                        break;
                    // Group Stop
                    case -2:
                    case 5:
                        open_groups.remove(point.node);
                        break;
                    // Layer Stop
                    case -1:
                    case 6:
                        open_nodes.get(point.layer).remove(point.node);
                        break;
                    default:
                        throw new RuntimeException("Bug!");
                }
            }

            return groups;
        }

        private List<Group> postprocess(List<Group> groups) {
            if(includeEmptyGroups) {
                return groups;
            }
            else {
                return groups.stream().filter(grp -> !grp.empty()).collect(Collectors.toList());
            }
        }

        public List<Group> solve() {
            List<Point> points = new ArrayList<>();
            addGroups(points);
            addLayers(points);

            // Sort the points
            Collections.sort(points);
            return postprocess(sweep(points));
        }
    }
}
