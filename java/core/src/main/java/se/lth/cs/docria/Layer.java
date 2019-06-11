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

import se.lth.cs.docria.exceptions.DataInconsistencyException;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Layer extends AbstractCollection<Node> {
    private Document parent;
    private Schema.Layer schema;
    private ArrayList<Node> storage = new ArrayList<>();
    private int size;
    protected NodeFactory nodeFactory;

    public Schema.Layer schema() {
        return schema;
    }

    /**
     * Get layer builder
     * @param name name of layer
     * @return layer builder
     */
    public static Schema.LayerBuilder builder(String name) {
        return Schema.layer(name);
    }

    /**
     * @deprecated use {@link #builder(String)}
     * @param name layer name
     * @return layer builder
     */
    @Deprecated
    public static Schema.LayerBuilder create(String name) {
        return builder(name);
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

    /**
     * @deprecated use {@link Node#builder(Layer) in combination with {@link Layer#add(Node)}}
     * @return
     */
    @Deprecated
    public Node.Builder create() {
        return new Node.Builder(this, this.nodeFactory.create());
    }

    public Node get(int id) {
        return storage.get(id);
    }

    public Layer addField(String name, DataType type) {
        if(schema.fields().containsKey(name))
            throw new IllegalArgumentException("Layer " + name() + " already contains field: " + name);

        schema.add(name, type);
        return this;
    }

    public Layer removeField(String name) {
        if(!schema.fields().containsKey(name)) {
            return this;
        }

        schema.remove(name);

        for (Node node : this) {
            if(node.has(name)) {
                node.remove(name);
            }
        }

        return this;
    }

    public Optional<Node> first() {
        return this.storage.stream().filter(Objects::nonNull).findFirst();
    }

    public Optional<Node> last() {
        final int sz = this.storage.size()-1;
        return IntStream.range(0,this.storage.size()).map(i -> sz - i).mapToObj(this.storage::get).filter(Objects::nonNull).findFirst();
    }

    /**
     * Sorts nodes in this layer by the given comparator, compacts the collection pre sorting.
     * @param comparator the comparator to use for sorting
     * @return this instance
     */
    public Layer sort(Comparator<Node> comparator) {
        compact();
        storage.subList(0, size).sort(comparator);
        return this;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public void clear() {
        for (Node node : storage) {
            node.layer = null;
            node.id = -1;
        }
        storage.clear();
        this.size = 0;
    }

    @Override
    public boolean add(Node node) {
        Objects.requireNonNull(node);
        // TODO: In case of specific node factory, convert/verify incoming type

        if(node.layer != null)
            throw new IllegalArgumentException("Cannot add node which is attached to an existing layer!");

        node.id = storage.size();
        node.layer = this;
        if(this.size < storage.size()) {
            storage.set(this.size, node);
            this.size++;
            return true;
        }
        else
        {
            this.size++;
            return storage.add(node);
        }
    }

    @Override
    public Iterator<Node> iterator() {
        return stream().iterator();
    }

    public Node remove(Node n) {
        Objects.requireNonNull(n);
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

    /**
     * Return a stream of the nodespan
     * @param start the left node, node with lowest id
     * @param stop the right node, node with highest id, inclusive.
     * @return Stream of nodes in range
     */
    public Stream<Node> nodespan(Node start, Node stop) {
        Objects.requireNonNull(start);
        Objects.requireNonNull(stop);
        if(start.layer != this)
            throw new DataInconsistencyException(
                    String.format("Start node ( %s ) is not bound to this layer: %s", start, this));

        if(stop.layer != this)
            throw new DataInconsistencyException(
                    String.format("Stop node ( %s ) is not bound to this layer: %s", stop, this));

        if(start.id > stop.id)
            throw new DataInconsistencyException(
                    String.format("Start and stop are not in sequence: start.id = %d, stop.id = %d", start.id, stop.id)
            );

        return IntStream.range(start.id, stop.id+1).mapToObj(this.storage::get).filter(Objects::nonNull);
    }

    @Override
    public boolean contains(Object o) {
        if(!(o instanceof Node))
            return false;

        Node n = (Node)o;
        return n.layer == this;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        Objects.requireNonNull(c);

        boolean edited = false;
        for (Object o : c) {
            if(!(o instanceof Node))
                throw new IllegalArgumentException("Collection must only contain nodes!");

            Node n = (Node)o;
            if(n.layer != this)
                throw new IllegalArgumentException(String.format("Collection must only contain nodes bound to this layer: %s", name()));

            n.id = -n.id;
            edited = true;
        }

        if(edited) {
            for (int i = 0; i < this.storage.size(); i++) {
                Node node = this.storage.get(i);
                if (node != null) {
                    if (node.id >= 0) {
                        this.storage.set(i, null);
                        this.size--;
                    } else {
                        node.id = -node.id;
                    }
                }
            }

            if (this.size < 0.75 * storage.size() && storage.size() > 16)
                compact();
        }

        return edited;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean ops = false;
        for (Object o : c) {
            ops |= this.remove(o);
        }
        return ops;
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
        int i = 0;
        for (int k = 0; k < storage.size(); k++) {
            if(storage.get(k) != null) {
                Node node = storage.get(k);
                node.id = i;
                storage.set(i++, node);
            }
        }

        for(int k = size; k < storage.size(); k++) {
            storage.set(k, null);
        }

        // Shrink to size
        if(this.size < 0.75 * storage.size() && storage.size() > 16)
            storage = new ArrayList<>(storage.subList(0, this.size));
    }

    @Override
    public void forEach(Consumer<? super Node> action) {
        Objects.requireNonNull(action);
        this.storage.forEach(n -> {if(n != null) action.accept(n); });
    }

    @Override
    public int size() {
        return size;
    }
}
