package se.lth.cs.docria.values;

import se.lth.cs.docria.DataType;
import se.lth.cs.docria.Layer;
import se.lth.cs.docria.Node;
import se.lth.cs.docria.Value;

import java.util.List;

public class NodeArrayValue extends Value {
    private final Node[] nodes;

    public NodeArrayValue(Node[] nodes) {
        if(nodes == null)
            throw new NullPointerException("nodes");

        if(nodes.length == 0)
            throw new IllegalArgumentException("Nodes has to contain at least one node.");

        Layer layer = nodes[0].layer();
        for (Node node : nodes) {
            if(node.layer() != layer) {
                throw new IllegalArgumentException("nodes is not consistent, at least one node points a different layer: " + node.layer().name());
            }
        }

        this.nodes = nodes;
    }

    @Override
    public DataType type() {
        return nodes[0].layer().schema().layerArrayType();
    }

    public NodeArrayValue(List<? extends Node> nodes) {
        this.nodes = nodes.toArray(new Node[nodes.size()]);
    }

    @Override
    public String stringValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node nodeValue() {
        if(nodes.length == 1)
            return nodes[0];
        else
            throw new UnsupportedOperationException();
    }

    @Override
    public Node[] nodeArrayValue() {
        return nodes;
    }

    @Override
    public void visit(ValueVisitor visitor) {
        visitor.accept(this);
    }
}
