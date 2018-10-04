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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class ConcreteNode extends Node {
    public static class NodeInterface {
        public final Map<String, Function<Node, Value>> getters;
        public final Map<String, BiConsumer<Node, Value>> setters;

        public NodeInterface(Map<String, Function<Node, Value>> getters, Map<String, BiConsumer<Node, Value>> setters) {
            this.getters = getters;
            this.setters = setters;
        }
    }

    public static class Builder<T extends Node> {
        public final Map<String, Function<Node, Value>> getters = new TreeMap<>();
        public final Map<String, BiConsumer<Node, Value>> setters = new TreeMap<>();

        public Builder<T> addField(String name, Function<T, Value> getter, BiConsumer<T, Value> setter) {
            getters.put(name, (Function<Node, Value>)getter);
            setters.put(name, (BiConsumer<Node, Value>)setter);
            return this;
        }

        public NodeInterface build() {
            return new NodeInterface(getters, setters);
        }
    }

    @Override
    public void forEach(BiConsumer<String, Value> fieldFunction) {
        super.forEach(fieldFunction);
        NodeInterface nodeInterface = nodeInterface();
        nodeInterface.getters.forEach((k,v) -> fieldFunction.accept(k, v.apply(this)));
    }

    @Override
    protected void setData(TreeMap<String, Value> data) {
        Map<String, BiConsumer<Node, Value>> setters = nodeInterface().setters;

        setters.forEach((k,v) -> {
            if(data.containsKey(k)) {
                v.accept(this, data.get(k));
                data.remove(k);
            }
        });

        super.setData(data);
    }

    @Override
    public Value get(String field) {
        Map<String, Function<Node, Value>> getters = nodeInterface().getters;
        if(getters.containsKey(field))
            return getters.get(field).apply(this);
        else
            return super.get(field);
    }

    @Override
    public Node put(String field, Value value) {
        Map<String, BiConsumer<Node, Value>> setters = nodeInterface().setters;
        if(setters.containsKey(field)) {
            setters.get(field).accept(this, value);
            return this;
        }
        else
            return super.put(field, value);
    }

    @Override
    public boolean has(String field) {
        return nodeInterface().getters.containsKey(field) || super.has(field);
    }

    protected abstract NodeInterface nodeInterface();
}
