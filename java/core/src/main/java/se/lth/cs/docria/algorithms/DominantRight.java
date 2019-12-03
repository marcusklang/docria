package se.lth.cs.docria.algorithms;

import se.lth.cs.docria.Layer;
import se.lth.cs.docria.Node;
import se.lth.cs.docria.NodeSpan;
import se.lth.cs.docria.Span;

import java.util.*;
import java.util.stream.Collectors;

public class DominantRight {
    public static class Segment<T> {
        private final int start;
        private final int stop;
        private final T tag;

        public Segment(int start, int stop, T tag) {
            this.start = start;
            this.stop = stop;
            this.tag = tag;
        }

        public int getStart() {
            return start;
        }

        public int getStop() {
            return stop;
        }

        public T getTag() {
            return tag;
        }

        public int length() {
            return stop-start;
        }
    }

    private static class Point<T> implements Comparable<Point<T>> {
        private final Segment<T> segment;
        private final int offset;
        private final boolean start;

        public Point(Segment<T> segment, int offset, boolean start) {
            this.segment = segment;
            this.offset = offset;
            this.start = start;
        }

        @Override
        public int compareTo(Point<T> o) {
            int res = Integer.compare(offset, o.offset);
            if(res == 0)
                res = -Boolean.compare(start, o.start);

            return res;
        }
    }

    public static <T> List<T> resolve(Iterable<Segment<T>> segments) {
        List<Point<T>> points = new ArrayList<>();
        segments.forEach(seg -> {
            if(seg.start != seg.stop) {
                points.add(new Point<>(seg, seg.start, true));
                points.add(new Point<>(seg, seg.stop-1, false));
            } else {
                points.add(new Point<>(seg, seg.start, true));
                points.add(new Point<>(seg, seg.stop, false));
            }
        });

        Collections.sort(points);

        List<T> output = new ArrayList<>();

        Segment<T> open = null;
        for (Point<T> point : points) {
            if(open != null) {
                if(point.start) {
                    if(point.segment.length() >= open.length()) {
                        open = point.segment;
                    }
                } else if(open == point.segment) {
                    output.add(point.segment.tag);
                    open = null;
                }
            } else {
                open = point.segment;
            }
        }

        return output;
    }


    public static List<Node> resolveNodespans(Collection<Node> nodes, String nodespanField) {
        return resolve(() -> nodes.stream().map(n -> {
            NodeSpan span = n.get(nodespanField).nodeSpanValue();
            return new Segment<>(span.getLeft().id(), span.getRight().id()+1, n);
        }).iterator());
    }

    public static List<Node> resolve(Collection<Node> nodes, String spanfield) {
        return resolve(() -> nodes.stream()
                                  .map(n -> {
                                      Span span = n.get(spanfield).spanValue();
                                      return new Segment<>(span.start(), span.stop(), n);
                                  }).iterator());
    }
}
