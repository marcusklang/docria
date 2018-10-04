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

import java.util.*;
import java.util.stream.Stream;

public class Layer extends AbstractCollection<Node> {
    private Document parent;
    private Schema.Layer schema;
    private ArrayList<Node> storage = new ArrayList<>();
    private int size;
    private NodeFactory nodeFactory;

    public Schema.Layer schema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Layer nodes = (Layer) o;

        if (!parent.equals(nodes.parent)) return false;
        return schema.equals(nodes.schema);
    }

    @Override
    public int hashCode() {
        int result = parent.hashCode();
        result = 31 * result + schema.hashCode();
        return result;
    }

    public Layer(Document parent, Schema.Layer schema) {
        this.parent = parent;
        this.schema = schema;
        this.nodeFactory = schema.factory();
    }

    public String name() {
        return schema.name();
    }

    @Override
    public Stream<Node> stream() {
        return storage.stream().filter(Objects::nonNull);
    }

    public <T> Stream<T> stream(Class<T> nodeType) {
        return (Stream<T>)storage.stream().filter(Objects::nonNull);
    }

    public Node.Builder create() {
        return new Node.Builder(this, this.nodeFactory.create());
    }

    public Node get(int id) {
        return storage.get(id);
    }

    @Override
    public boolean add(Node node) {
        // TODO: In case of specific node factory, convert/verify incoming type

        if(node.layer != null)
            throw new IllegalArgumentException("Cannot add node which is attached to an existing layer!");

        node.id = storage.size();
        node.layer = this;
        this.size++;
        return storage.add(node);
    }

    @Override
    public Iterator<Node> iterator() {
        return stream().iterator();
    }

    public Node remove(Node n) {
        if(n.layer != this) {
            throw new IllegalStateException("node is not associated with this layer.");
        }

        storage.set(n.id, null);
        n.layer = null;
        this.size--;

        if(this.size < 0.75 * storage.size() && storage.size() > 16)
            compact();

        return n;
    }

    @Override
    public boolean remove(Object o) {
        if(!(o instanceof Node)) {
            return false;
        }

        Node n = (Node)o;
        if(n.layer != this) {
            return false;
        }

        n.layer = null;
        storage.set(n.id, null);
        this.size--;

        if(this.size < 0.75 * storage.size() && storage.size() > 16)
            compact();

        return true;
    }

    public void compact() {
        //TODO: Shrink capacity as needed

        int i = 0;
        for (int k = 0; k < storage.size(); k++) {
            if(storage.get(k) != null) {
                Node node = storage.get(k);
                node.id = i;
                storage.set(i++, node);
            }
        }
    }

    @Override
    public int size() {
        return size;
    }
}
