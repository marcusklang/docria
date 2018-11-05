# -*- coding: utf-8 -*-
#
# Copyright 2018 Marcus Klang (marcus.klang@cs.lth.se)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from docria.model import Node, NodeLayerCollection, TEnum
from typing import Set, List, Callable, Tuple, Dict, Optional, Iterator
from collections import deque
import functools


def parent_span_cover(parent_nodes,  children_nodes, parent_span_prop="text", children_span_prop="text")->List[Tuple[Node, List[Node]]]:
    """Find all (parent, list(children)) span covers."""
    raise NotImplementedError()


def parent_span_overlap(parent_nodes,  children_nodes, parent_span_prop="text", children_span_prop="text")->List[Tuple[Node, List[Node]]]:
    """Find all (parent, list(children)) span overlaps/intersections"""
    raise NotImplementedError()


def shortest_path(start: Node, target: Node, children: Callable[[Node], Iterator[Node]], cost: Callable[[Node,Node], bool])->List[Node]:
    """Shortest path between nodes"""
    raise NotImplementedError()


def get_prop(prop, default=None):
    """First order function which can be used to extract property of nodes"""
    def get(n: Node):
        return default if prop not in n else n[prop]

    return get


def chain(*fns):
    """Create a new function for a sequence of functions which will be applied in sequence"""
    def forward(x):
        for fn in fns:
            x = fn(x)

        return x
    return forward


def children_of(layer: NodeLayerCollection, *props):
    """Get children of a given property

    Note: the code will check against schema if it is an array or single node."""
    def node_iter(prop):
        def yielder(n: Node):
            if prop in n:
                yield n[prop]

        return yielder

    def node_array_iter(prop):
        def yielder(n: Node):
            if prop in n:
                return iter(n[prop])
            else:
                return iter(()) # Empty iterator

        return yielder

    def compose(yielders):
        def yielder(n: Node):
            for yielder in yielders:
                for n in yielder(n):
                    yield n

        return yielder

    if len(props) == 1:
        typedef = layer.schema.fields[props[0]]
        if typedef.typename == TEnum.NODEREF:
            return node_iter(props[0])
        elif typedef.typename == TEnum.NODEREF_MANY:
            return node_array_iter(props[0])
        else:
            raise ValueError("field %s has type %s which is not supported as a children type" % (
                props[0], repr(typedef))
            )

    elif len(props) > 1:
        yielders = []
        for prop in props:
            typedef = layer.schema.fields[prop]
            if typedef.typename == TEnum.NODEREF:
                yielders.append(node_iter(prop))
            elif typedef.typename == TEnum.NODEREF_MANY:
                yielders.append(node_array_iter(prop))
            else:
                raise ValueError("field %s has type %s which is not supported as a children type" % (
                    prop, repr(typedef))
                )

        return compose(yielders)
    else:
        raise ValueError("props has to contain at least one property!")


def bfs(start: Node,
        children: Callable[[Node], Iterator[Node]],
        is_result: Optional[Callable[[Node], bool]]=None)->Iterator[Tuple[int, Node]]:
    """
    Breadth first search

    :param start: the start node
    :param children: function returning children iterator for given node
    :param is_result: optional, function indicating if node should be emitted, default is true for all.
    :return iterator of found nodes with depth during search
    """
    visited = set()
    queue = deque()
    queue.append((0, start))
    while queue:
        current_depth, current_node = queue.popleft()
        if current_node in visited:
            continue

        visited.add(current_node)

        if is_result and is_result(current_node):
            yield current_depth, current_node
        elif not is_result:
            yield current_depth, current_node

        for child in children(current_node):
            if child not in visited:
                queue.append((current_depth+1, child))


def dfs(start: Node,
        children: Callable[[Node], Iterator[Node]],
        is_result: Optional[Callable[[Node], bool]]=None)->Iterator[Node]:
    """
    Depth first search

    :param start: start node
    :param children: function returning children iterator for given node
    :param is_result: optional, function indicating if node should be emitted, default is true for all.
    :return iterator of nodes found during search
    """
    visited = set()
    stack = list()
    stack.append(start)

    while stack:
        current = stack.pop()
        if current in visited:
            continue

        visited.add(current)
        if is_result and is_result(current):
            yield current
        elif not is_result:
            yield current

        child_nodes = [ch for ch in children(current) if ch not in visited]
        child_nodes.reverse()
        stack.extend(child_nodes)


def dfs_leaves(start: Node,
                 children: Callable[[Node], Iterator[Node]],
                 is_result: Optional[Callable[[Node], bool]]=None)->Iterator[Node]:
    """
    Depth first search, only returning the leaves i.e. those without children or outgoing links

    :param start: start node
    :param children: function returning children iterator for given node
    :param is_result: optional, function indicating if node should be emitted, default is true for all.
    :return iterator of nodes found during search
    """
    visited = set()
    stack = list()
    stack.append(start)

    while stack:
        current = stack.pop()
        if current in visited:
            continue

        visited.add(current)

        child_nodes = [ch for ch in children(current) if ch not in visited]
        child_nodes.reverse()

        if not child_nodes and is_result and is_result(current):
            yield current
        elif not child_nodes and not is_result:
            yield current

        stack.extend(child_nodes)
