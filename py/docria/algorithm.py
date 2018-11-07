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

from docria.model import Document, Node, NodeLayerCollection, TEnum
from typing import Set, List, Callable, Tuple, Dict, Optional, Iterator
from collections import deque
import functools


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


def span_translate(doc: Document,
                   mapping_layer: str, target_source_map: Tuple[str,str],
                   layer_remap: str, source_target_remap: Tuple[str, str]):

    target_pos, source_pos = target_source_map
    target_pos_remap, source_pos_remap = source_target_remap

    mapping_layer = doc.layer[mapping_layer]
    assert mapping_layer.schema.fields[target_pos].typename == TEnum.SPAN
    assert mapping_layer.schema.fields[source_pos].typename == TEnum.SPAN

    layer_remap = doc.layer[layer_remap]
    assert layer_remap.schema.fields[source_pos_remap].typename == TEnum.SPAN
    assert layer_remap.schema.fields[target_pos_remap].typename == TEnum.SPAN

    target_text = doc.texts[mapping_layer.schema.fields[target_pos].options["context"]]

    # 1. Find start/end point interval intersections against mapping
    # 1.1 Produce mapping array
    mapping_in_source = []

    for m in mapping_layer:
        sourceStart = m[source_pos].start
        sourceStop  = m[source_pos].stop

        mapping_in_source.append(((sourceStart, 0), (0, m)))
        mapping_in_source.append(((sourceStop, -1), (0, m)))

    for n in layer_remap:
        if not target_pos_remap in n:
            sourceRemapStart = n[source_pos_remap].start
            sourceRemapStop = n[source_pos_remap].stop-1

            mapping_in_source.append(((sourceRemapStart, 1), (1, n)))
            mapping_in_source.append(((sourceRemapStop, 2), (1, n)))

    mapping_in_source.sort(key=lambda tup: tup[0])

    # 2. Translate points with relative distance from start in interval
    remap_start_offsets = {}

    active_interval_start = None
    active_interval_target_start = None
    active_node = None
    for pos, source in mapping_in_source:
        marker, markertype = pos
        sourcetype, node = source

        if sourcetype == 0:
            if markertype == -1:
                # End
                active_interval_start = None
                active_interval_target_start = None
                active_node = None
            elif markertype == 0:
                assert active_interval_start is None, "Overlapping mapping is not allowed!"
                active_interval_start = marker
                active_interval_target_start = node[target_pos].start
                active_node = node
            else:
                assert False, "Bug! Should never happen."
        elif sourcetype == 1:
            assert active_interval_start is not None, "Current position %d is outside any " \
                                                "mapping interval, i.e. there is a gap in the mapping!" % marker

            if markertype == 1:
                remap_start_offsets[node._id] = (marker - active_interval_start) + active_interval_target_start
            elif markertype == 2:
                assert node._id in remap_start_offsets, "Start was not encountered, possibly input data invalid or bug!"

                stopOffset = (marker - active_interval_start) + active_interval_target_start + 1
                startOffset = remap_start_offsets[node._id]

                node[target_pos_remap] = target_text[startOffset:stopOffset]
