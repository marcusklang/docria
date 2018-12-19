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

from docria.model import Document, Node, NodeLayerCollection, TEnum, TextSpan
from typing import Set, List, Callable, Tuple, Dict, Optional, Iterator, Iterable, Any
from collections import deque, namedtuple, defaultdict
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
    """
    Translate span ranges from a partial extraction to the original data.

    Target is the original data, Source is the partial extraction ranges.

    :param doc: document
    :param mapping_layer: the layer which contains the mapping
    :param target_source_map: tuple of (target field, source field)
    :param layer_remap: the layer which should be mapped
    :param source_target_remap: tuple of (source field, target field)
    """
    target_pos, source_pos = target_source_map
    source_pos_remap, target_pos_remap = source_target_remap

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
        if target_pos_remap not in n:
            sourceRemapStart = n[source_pos_remap].start
            sourceRemapStop = n[source_pos_remap].stop-1

            mapping_in_source.append(((sourceRemapStart, 1), (1, n)))
            mapping_in_source.append(((sourceRemapStop, 2), (1, n)))

    mapping_in_source.sort(key=lambda tup: tup[0])

    # 2. Translate points with relative distance from start in interval
    remap_start_offsets = {}

    active_interval_start = None
    active_interval_target_start = None
    for pos, source in mapping_in_source:
        marker, markertype = pos
        sourcetype, node = source

        if sourcetype == 0:
            if markertype == -1:
                # End
                active_interval_start = None
                active_interval_target_start = None
            elif markertype == 0:
                assert active_interval_start is None, "Mapping which overlaps is not allowed!"
                active_interval_start = marker
                active_interval_target_start = node[target_pos].start
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


def is_covered_by(span_a: TextSpan, span_b: TextSpan)->bool:
    """
    Covered by predicate
    :param span_a: the node that is tested for cover
    :param span_b: the node that might cover span_a
    :return: true or false
    """
    return span_a.start >= span_b.start and span_a.stop <= span_b.stop


def group_by_span(group_nodes: List[Node],
                  layer_nodes: Dict[str, Iterable[Node]],
                  resolution="intersect",
                  group_span_field ="text",
                  layer_span_field=None,
                  include_empty_groups=True)\
        ->List[Tuple[Node, Dict[str, List[Node]]]]:
    """
    Groups all nodes in layer_nodes into the corresponding bucket_node

    Nodes with spans that equals to NIL/None are ignored.

    :param group_nodes: the nodes to group by
    :param layer_nodes: the nodes to assign to zero or more groups
    :param resolution:  which resolution algorithm that shall be used: intersect or cover
                        intersect: the identity function for resolutions (all intersects are grouped)
                        cover: imposes a requirement that the group node must fully
                               cover the layer node (node_start >= group_start and node_stop <= group_stop)
    :param group_span_field: name of span property name, defaults to text
    :param layer_span_field: layer to span property name, defaults to text
    :param include_empty_groups: include groups which does not contain any matching layer nodes
    """
    if layer_span_field is None:
        layer_span_field = defaultdict(lambda: "text")

    node_list = []  # type: List[Tuple[Tuple[int, int], Tuple[Optional[str], Node]]]

    # 1. Convert all nodes to Start, Stop symbols with added context information
    for group_node in group_nodes:
        if group_span_field in group_node:
            span = group_node[group_span_field]  # type: TextSpan

            if span.start == span.stop:
                # singleton
                node_list.append(((span.start, 3), (None, group_node)))
                node_list.append(((span.stop,  4), (None, group_node)))
            else:
                node_list.append(((span.start, 0), (None, group_node)))
                node_list.append(((span.stop, -2), (None, group_node)))

    for layer_name, layer in layer_nodes.items():
        try:
            span_name = layer_span_field[layer_name]
        except KeyError as e:
            raise KeyError("Could not find span property name for layer: %s" % layer_name) from e

        for layer_node in layer:
            if span_name in layer_node:
                span = layer_node[span_name]  # type: TextSpan

                if span.start == span.stop:
                    # singleton
                    node_list.append(((span.start, 3), (layer_name, layer_node)))
                    node_list.append(((span.stop,  4), (layer_name, layer_node)))
                else:
                    node_list.append(((span.start, 1), (layer_name, layer_node)))
                    node_list.append(((span.stop, -1), (layer_name, layer_node)))

    # 2. Sort by start, stop
    node_list.sort(key=lambda tup: tup[0])

    node_list_groups = []  # type: List[List[Tuple[Tuple[int,int], Tuple[Optional[str], Node]]]]
    current_group = None
    for tup in node_list:
        if tup[0] is not None and current_group == tup[0]:
            node_list_groups[-1].append(tup)
        else:
            node_list_groups.append([tup])

    # 3. Run sweep, and assign all groups relevant nodes
    groups = dict()  # type: Dict[Node, Dict[str, List[Node]]]
    group_list = list()  # type: List[Tuple[Node, Dict[str, List[Node]]]]
    open_nodes = set()
    open_groups = set()

    k = 0
    while k < len(node_list_groups):
        nodes = node_list_groups[k]
        if 0 <= nodes[0][0][1] < 4:
            # Group start
            for _, (layer, node) in nodes:
                if layer is not None:
                    open_nodes.add((layer, node))
                    for open_group in open_groups:
                        groups[open_group][layer].append(node)
                else:
                    group_dict = {k: [] for k in layer_nodes.keys()}
                    groups[node] = group_dict
                    group_list.append((node, group_dict))
                    open_groups.add(node)

                    for open_layer, open_node in open_nodes:
                        group_dict[open_layer].append(open_node)
        else:
            # Group stop
            for _, (layer, node) in nodes:
                if layer is not None:
                    open_nodes.remove((layer, node))
                else:
                    open_groups.remove(node)

        k += 1

    # 4. Apply resolution algorithm if necessary
    if resolution == "cover":
        for i in range(len(group_list)):
            group_node, layer_group_nodes = group_list[i]
            group_span = group_node[group_span_field]
            group_list[i] = (group_node, {
                k: [n for n in v if is_covered_by(n[layer_span_field[k]], group_span)]
                for k, v in layer_group_nodes.items()
            })

    # 5. Final filtering if necessary
    if not include_empty_groups:
        group_list = [grp for grp in group_list if sum(map(len, grp[1].values())) > 0]

    # 4. Return result
    return group_list


def dominant_right(segments: List[Tuple[int, int, Any]])->List[Any]:
    segment_list = []
    for tup in segments:
        start, stop, item = tup
        segment_list.append(((start, 0), tup))
        if start != stop:
            segment_list.append(((stop-1, 1), tup))
        else:
            segment_list.append(((stop, 1), tup))

    segment_list.sort(key=lambda el: el[0])

    segment_output = []
    open_node = None

    for ((off, mode), tup) in segment_list:
        if mode == 0:
            if open_node is not None:
                start, stop, item = open_node

                if stop-start <= tup[1]-tup[0]:
                    open_node = tup
            else:
                open_node = tup
        else:
            if open_node is tup:
                segment_output.append(tup[2])
                open_node = None

    return segment_output


def dominant_right_span(nodes: Iterable[Node], spanfield: str)->List[Node]:
    segments = [(n[spanfield].start, n[spanfield].stop, n) for n in nodes if spanfield in n]
    return dominant_right(segments)
