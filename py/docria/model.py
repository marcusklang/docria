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
"""Docria document model ( **primary module** )"""

from typing import Dict, List, Tuple, Callable, Any, Iterator, Iterable, Union, Set, Optional, Sized
from enum import Enum
from .query import *


class SchemaValidationError(Exception):
    """Schema validation failed"""
    def __init__(self, message, fields):
        super().__init__(message)
        self.fields = fields


class SchemaError(Exception):
    """Failed to validate a part of the schema"""
    def __init__(self, message):
        super().__init__(message)


class DataValidationError(Exception):
    """Failed to validate document"""
    def __init__(self, message):
        super().__init__(message)


class _DictAsMembers:
    def __init__(self, data):
        self.__data__ = data

    def __getattr__(self, item):
        return self.__data__.get(item)

    def __setattr__(self, key, value):
        if key == "__data__":
            super().__setattr__(key, value)
        else:
            self.__data__[key] = value

    def __delattr__(self, item):
        del self.__data__[item]


class Node(dict):
    """
    Basic building block of the document model

    :Example:
    >>> from docria.model import Document, DataTypes as T, Node
    >>>
    >>> doc = Document()
    >>> tokens = doc.add_layer("token", pos=T.string)
    >>>
    >>> node = Node(pos="NN")
    >>>
    >>> tokens.add_many([ node ])
    >>>
    >>> print(node["pos"])  # Gets the field of pos
    >>> print(node.get("pos"))  # Node works like a dictionary
    >>> print(node.keys())  # return set fields
    >>> print("pos" in node)  # check if pos field is set.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._id = -1  # type: int
        self.collection = None  # type: NodeLayerCollection

    @property
    def i(self):
        """
        Get the index of this node.

        :return: -1 if not bound to a layer, [0,) if bound in a layer
        """
        assert self._id != -1, "Node is not bound to any node collection!"
        return self._id

    def with_id(self, id):
        """
        Utility method to set id and return this node.
        This is an unsafe method and should only be used when you know what you are doing.

        :param id: internal id
        :return: self
        """
        self._id = id
        return self

    @property
    def fld(self):
        """Get a pythonic wrapper for this node .e.g node.fld.id == node["id"] """
        return _DictAsMembers(self)

    def detach(self):
        """Remove itself from the document model"""
        self.collection.remove(self)

    def is_dangling(self)->bool:
        """Check if this node is dangling i.e. is not attached to an existing layer, possibly removed or never added."""
        return self.collection is None

    def is_valid(self, noexcept=True)->bool:
        """
        Validate this node against schema

        :param noexcept: set to False if exceptions should be raised if validation failure,
                         this will give the exact cause of validation failure.

        :return: true if valid
        """
        if noexcept:
            try:
                return self.collection.validate(self)
            except AssertionError:
                return False
            except SchemaValidationError:
                return False
        else:
            return self.collection.validate(self)

    def _ipython_key_completions_(self):
        return list(self.keys())

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return self is other

    def __str__(self):
        tbl = self._table_repr_()
        return tbl.render_text()

    @property
    def left(self)->Union[None, "Node"]:
        """Get the node left of this node"""
        return self.collection.left(self)

    @property
    def right(self)->Union[None, "Node"]:
        """Get the node right of this node"""
        return self.collection.right(self)

    def iter_span(self, node: "Node"):
        """
        Return iterator which will give the span from this node to the given node

        :param node: target node (inclusive)

        :note:
        This method corrects for order, i.e. if the target node is to the left of this node, \
        the returned iterator will start at target node.
        """
        if self.i <= node.i:
            return self.collection.iter_nodespan(self, node)
        else:
            return self.collection.iter_nodespan(node, self)

    def _table_repr_(self):
        from docria.printout import Table, get_representation
        tbl = Table(hide_index=True,
                    caption="Node %s#%d" % (self.collection.schema.name, self._id) if len(self) > 0 else "")

        tbl.set_header("field", "value")

        for k, v in self.items():
            tbl.add_body(k, get_representation(v))

        return tbl

    def _repr_html_(self):
        tbl = self._table_repr_()
        return tbl.render_html()

    def __repr__(self):
        return "Node<%s#%d>" % (self.collection.schema.name, self._id) if len(self) > 0 else ""


class NodeCollection(Sized):
    def __init__(self, fieldtypes: Dict[str, "DataType"]):
        self.fieldtypes = fieldtypes

    def _table_repr_(self, title="Node collection with N=%d elements", offset=None):
        fields = self.fieldtypes.keys()

        fields = sorted(fields)
        from docria.printout import Table, TableRow, get_representation
        tbl = Table(title % len(self), hide_index=True)

        tbl.set_header(*fields)
        if offset is not None:
            for i, n in enumerate(self):
                values = list(map(lambda k: get_representation(n.get(k, None)), fields))
                tbl.add_body(TableRow(*values, index=i+offset))
        else:
            for n in self:
                values = list(map(lambda k: get_representation(n.get(k, None)), fields))
                tbl.add_body(TableRow(*values))

        return tbl

    def to_list(self)->"NodeList":
        """
        Convert this collection to a NodeList containing all node references

        :return: NodeList with all nodes in this layer
        """
        return NodeList(iter(self), fieldtypes=self.fieldtypes)

    def validate(self):
        for n in self:
            for field, dtype in self.fieldtypes.items():
                if field in n:
                    assert dtype.is_valid(n[field]), \
                        "Field %s, with value %s is not valid in node: %s" % \
                        (field, repr(n[field]), repr(n))

    def first(self):
        return next(iter(self), None)

    def last(self):
        last = None
        for n in self:
            last = n
        return last

    def _repr_html_(self):
        return self._table_repr_().render_html()

    def __contains__(self, item):
        return item in self.fieldtypes

    def __iter__(self):
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError

    def __getitem__(self, item):
        if isinstance(item, str):
            assert item in self.fieldtypes, "Field not in this collection"
            return NodeFieldCollection(self, item)
        elif callable(item):
            return NodeCollectionQuery(self, item)
        else:
            raise NotImplementedError("NodeCollections by default only support string indices for fields")


class NodeFieldCollection(Sized):
    """Field from a node collection"""
    def __init__(self, collection: NodeCollection, field):
        self.collection = collection
        self.field = field

    @property
    def dtype(self):
        """Get the DataType for this field"""
        return self.collection.fieldtypes[self.field]

    def _repr_html_(self):
        pass

    def __len__(self):
        return len(self.collection)

    def __iter__(self):
        return map(lambda n: n.get(self.field), self.collection)

    def __getitem__(self, item):
        return self.collection[item][self.field]

    def to_list(self):
        """Convert this node field collection to a python list with field elements."""
        return [v for v in self]

    def filter(self, cond: Callable[[Any], bool]):
        """
        Generic filter function.

        :param cond: a callable which will be given the value of this field, it is expected to match filter semantics.
        :return: filter predicate
        """
        return NodeFieldPredicateLambda(self.field, cond)

    def is_none(self):
        """
        Is none predicate, field value is none
        :return: is none predicate
        """
        return self.filter(lambda v: v is None)

    def has_value(self):
        """
        Has value predicate, does a field value exist
        :return: has value predicate
        """
        return self.filter(None.__ne__)

    def is_any(self, *item):
        """
        Is any of predicate, does field value exist in given items.

        :param item: the items to verify against
        :return: is any predicate
        """
        return NodeFieldPredicateContains(self, set(item))

    def covered_by(self, *range):
        """
        Covered by predicate

        :param range: tuple of start, stop
        :return: covered by predicate
        """
        assert self.dtype.typename == DataTypeEnum.SPAN, "Not a textspan field: %s" % repr(self)
        assert len(range) == 2, "range should be tuple of (start, stop)"
        cover_span = tuple(range)
        return self.filter(lambda span: span.covered_by(cover_span))

    def intersected_by(self, *range):
        """
        Intersected by predicate

        :param range: tuple of start, stop
        :return: intersected by predicate
        """
        assert self.dtype.typename == DataTypeEnum.SPAN, "Not a textspan field: %s" % repr(self)
        assert len(range) == 2, "range should be tuple of (start, stop)"
        cover_span = tuple(range)
        return self.filter(lambda span: span.intersected_by(cover_span))

    def __gt__(self, other):
        return NodeFieldPredicateGt(self, other)

    def __le__(self, other):
        return NodeFieldPredicateLe(self, other)

    def __lt__(self, other):
        return NodeFieldPredicateLt(self, other)

    def __ge__(self, other):
        return NodeFieldPredicateGe(self, other)

    def __ne__(self, other):
        return NodeFieldPredicateNeq(self, other)

    def __eq__(self, other):
        return NodeFieldPredicateEq(self, other)

    def _table_repr_(self):
        from docria.printout import Table, TableRow

        table = Table(caption="Field %s in %s" % (self.field, repr(self.collection)))
        table.set_header(self.field)

        for v in self:
            table.add_body(TableRow(v))

        return table

    def _repr_html_(self):
        return self._table_repr_().render_html()

    def __repr__(self):
        return "NodeFieldCollection(%s, N=%d, collection=%s)" % (self.field, len(self), repr(self.collection))

    def __str__(self):
        return self._table_repr_().render_text()


class NodeSpan(NodeCollection):
    """Represents a span of nodes in a layer

    .. automethod:: __getitem__
    .. automethod:: __len__
    """
    def __init__(self, left_most_node: "Node", right_most_node: "Node"):
        super().__init__(left_most_node.collection.schema.fields)
        assert left_most_node is not None, "Left is None"
        assert right_most_node is not None, "Right is None"
        assert left_most_node.i <= right_most_node.i, "Nodes are not given in order!"
        assert left_most_node.collection is right_most_node.collection, "Both nodes must reside in the same layer"
        self.left = left_most_node
        self.right = right_most_node

    def __iter__(self):
        assert self.left.i <= self.right.i, "Nodes have changed order, this NodeSpan is now invalid!"
        return self.left.iter_span(self.right)

    def text(self, field="text")->str:
        """
        Return text from left to right
        :param field: the text span field to use
        :return: string
        """
        left_span = self.left[field]  # type: TextSpan
        right_span = self.right[field]  # type: TextSpan
        return left_span.text_to(right_span)

    def textspan(self, field="text"):
        """
        Return text from left to right
        :param field: the text span field to use
        :return: string
        """
        left_span = self.left[field]  # type: TextSpan
        right_span = self.right[field]  # type: TextSpan
        return left_span.span_to(right_span)

    def first(self):
        return self.left

    def last(self):
        return self.right

    def _repr_html_(self):
        return self._table_repr_(title="Node span with N=%d elements", offset=self.left.i).render_html()

    def __len__(self):
        """Computes the number of nodes currently contained within this node span.

        This function has complexity O(n)."""
        return sum(1 for _ in self)

    def __repr__(self):
        fields = self.left.collection.schema.fields
        if "text" in fields and fields["text"].typename == DataTypeEnum.SPAN:
            return "NodeSpan[%s: %d to incl. %d] = %s" % (
                self.left.collection.name, self.left.i, self.right.i, repr(self.text())
            )
        else:
            return "NodeSpan[%s: %d to incl. %d]" % (self.left.collection.name, self.left.i, self.right.i)

    def __str__(self):
        tbl = self._table_repr_()
        return tbl.render_text()


class NodeList(list, NodeCollection):
    """Python list enriched with extra indexing and presentation functionality for optimal use in Docria.

    .. automethod:: __getitem__
    """
    def __init__(self, *elems, fieldtypes=None):
        list.__init__(self, *elems)
        assert all(map(lambda n: isinstance(n, Node), self)), "NodeList only accepts Node objects, received: %s" % list.__repr__(self)

        if fieldtypes is None:
            fields = {}
            node_collections = set()
            for n in self:
                if n.collection is None:
                    for fld, value in n.items():
                        fields.setdefault(fld, set()).add(DataTypes.typeof(value))
                else:
                    node_collections.add(n.collection)

            for nc in node_collections:
                for fld, dtype in nc.fieldtypes.items():
                    fields.setdefault(fld, set()).add(dtype)

            output_types = {}
            for fld, dtypes in fields.items():
                if len(dtypes) > 1:
                    if len(set(map(lambda dtype: dtype.typename, dtypes))) == 1:
                        # Exactly the same type
                        output_types[fld] = next(iter(dtypes))
                    else:
                        resolved_type = None  # type: Optional[DataType]
                        for el in dtypes:
                            if resolved_type is None:
                                resolved_type = el
                            elif not resolved_type.cast_up_possible(el):
                                raise ValueError("Could not merge field types: "
                                                 "%s and %s for field '%s', collection %s" %
                                                 (repr(resolved_type), repr(el), fld, repr(self)))
                            else:
                                resolved_type = resolved_type.cast_up(el)
                else:
                    output_types[fld] = next(iter(dtypes))

            NodeCollection.__init__(self, output_types)
        else:
            NodeCollection.__init__(self, fieldtypes)

    def __iter__(self)->Iterator[Node]:
        return super().__iter__()

    def __getitem__(self, item):
        """Get field value by nnam, node by index, new lists using standard slices or a list of indices"""
        if isinstance(item, int):
            return list.__getitem__(self, item)
        elif isinstance(item, slice):
            return NodeList(list.__getitem__(self, item), fieldtypes=self.fieldtypes)
        elif isinstance(item, list):
            getter = list.__getitem__
            return NodeList(iter(getter(self, int(indx)) for indx in item), fieldtypes=self.fieldtypes)
        else:
            return NodeCollection.__getitem__(self, item)

    def first(self):
        if len(self) > 0:
            return list.__getitem__(self, 0)
        else:
            return None

    def last(self):
        if len(self) > 0:
            return list.__getitem__(self, -1)
        else:
            return None

    def __repr__(self):
        return repr(super())

    def _repr_html_(self):
        tbl = self._table_repr_()
        return tbl.render_html()

    def __str__(self):
        tbl = self._table_repr_()
        return tbl.render_text()


class Offset:
    """Text offset object"""
    def __init__(self, offset: int):
        self._id = -1
        self._refcnt = 0
        self.offset = offset

    def incref(self):
        self._refcnt += 1

    def __int__(self):
        return self.offset

    def __index__(self):
        return self.offset

    def __eq__(self, off):
        return off is self or self.offset == off.offset

    def __hash__(self):
        return hash(self.offset)


class TextSpan:
    """
    Text span, consisting of a start and stop offset.

    :note:
    Use str(span) to get a real string.
    """
    def __init__(self, text: "Text", start_offset: int, stop_offset: int):
        assert start_offset <= stop_offset, "start must be <= end"
        self.text = text
        self._start = start_offset
        self._stop = stop_offset
        self.start_offset = start_offset
        self.stop_offset = stop_offset

    @property
    def start(self)->int:
        return self._start

    @property
    def stop(self)->int:
        return self._stop

    def __len__(self):
        return self._stop-self._start

    def __hash__(self):
        return hash((id(self.text), self._start, self._stop))

    def __eq__(self, textrange: Union["TextSpan", str]):
        if isinstance(textrange, TextSpan):
            return (self.start, self.stop) == (textrange.start, textrange.stop)
        else:
            return str(self) == textrange

    def __getitem__(self, indx: Union[slice, Tuple[int, int], int]):
        if isinstance(indx, slice):
            if indx.step is not None and indx.step != 1:
                raise NotImplementedError("Only step == 1 are supported.")

            start, stop, step = indx.indices(len(self))
            return self.text[start+self.start:stop+self.start]
        elif isinstance(indx, tuple) and len(indx) == 2:
            return self.text[indx[0]+self.start:indx[1]+self.start]
        elif isinstance(indx, int):
            start, stop, step = slice(indx, indx+1).indices(len(self))
            return self.text.text[start+self.start]
        else:
            raise ValueError("Unsupported input indx: %s" % repr(indx))

    def text_to(self, right_span: "TextSpan")->str:
        """
        Helper function to return new TextSpan from this position to the given span
        :param right_span: right most span
        :return: TextSpan
        """
        return self.text.text[self.start:right_span.stop]

    def span_to(self, right_span: "TextSpan")->"TextSpan":
        """
        Helper function to return new TextSpan from this position to the given span
        :param right_span: right most span
        :return: TextSpan
        """
        return self.text[self.start:right_span.stop]

    def covered_by(self, span):
        """
        Checks if this span is covered by given span
        :param span: the span to be covered by
        :return: boolean indicating cover
        """
        if isinstance(span, TextSpan):
            return span.start <= self.start and span.stop >= self.stop
        elif isinstance(span, tuple):
            start, stop = span
            return start <= self.start and stop >= self.stop
        else:
            raise NotImplementedError("Unknown span type: %s" % repr(span))

    def intersected_by(self, span):
        """
        Checks if this span is intersected by given span
        :param span: the span to be intersected by
        :return: boolean indicating intersection
        """
        if isinstance(span, TextSpan):
            return span.stop > self.start and span.start < self.stop
        elif isinstance(span, tuple):
            start, stop = span
            return stop > self.start and start < self.stop
        else:
            raise NotImplementedError("Unknown span type: %s" % repr(span))

    def _trim_offsets(self):
        """
        Private function which finds trim offsets
        :return: None if no new span or (start, stop) if new span
        """
        if len(self) == 0:
            return None

        new_start = self.start
        new_stop = self.stop
        if str.isspace(self.text.text[self.start]):
            # move forward
            for w in str(self):
                if not str.isspace(w):
                    break

                new_start += 1
        elif str.isspace(self.text.text[self.stop-1]):
            # move backward
            for i in range(len(self)-1, -1, -1):
                if not str.isspace(self.text.text[new_stop-1]):
                    break

                new_stop -= 1

        if new_stop <= new_start:
            startoff = self._start
            stopoff = startoff

        elif new_start != self.start or new_stop != self.stop:
            startoff = self.text.offset(new_start)
            stopoff = self.text.offset(new_stop)
        else:
            return None

        return startoff, stopoff

    def trim_(self):
        """
        Trim this span in-place by removing whitespace, move start forward,
        stop backward until something which is not whitespace is encountered.
        :return self
        """
        offs = self._trim_offsets()
        if offs is not None:
            self._start, self._stop = offs

        return self

    def trim(self):
        """
        Return trimmed span range by whitespace, move start forward,
        stop backward until something which is not whitespace is encountered.
        :return self or new instance if new span
        """
        offs = self._trim_offsets()
        if offs is None:
            return self
        else:
            return TextSpan(self.text, offs[0], offs[1])

    def __repr__(self):
        return "span(%s[%d:%d]) = %s" % (
            self.text.name,
            self._start,
            self._stop,
            repr(self.text.text[self.start:self.stop])
        )

    def __iter__(self):
        return iter(str(self))

    def __str__(self):
        return self.text.text[self.start:self.stop]


class Text:
    """Text object, consisting of text and an index of current offsets"""
    def __init__(self, name, text):
        self.name = name
        self.text = text
        self.spantype = DataTypeTextspan(DataTypeEnum.SPAN, context=self.name)

    def __str__(self):
        """Convert to string"""
        return self.text

    def __len__(self):
        return len(self.text)

    def __iter__(self):
        return iter(self.text)

    def _repr_html_(self):
        from html import escape
        return "<h3>Text: {0}</h3><pre>{1}</pre>".format(self.name, escape(self.text))

    def compile(self, offsets: List[int]):
        """
        Compiles text for serialization

        :type offsets: the offsets including 0 and length of text
        :return: List of segments
        """

        output = []
        for i in range(len(offsets)-1):
            start = offsets[i]
            stop = offsets[i+1]

            output.append(self.text[start:stop])

        return output

    def offset(self, indx)->int:
        assert 0 <= indx <= len(self.text), "Offset %d not valid: " \
                                            "outside acceptable range [0, %d]" % (indx, len(self.text))
        return indx

    def __getitem__(self, indx):
        """Get a slice of the text"""
        if isinstance(indx, slice):
            if indx.step is not None and indx.step != 1:
                raise NotImplementedError("Only step == 1 are supported.")

            start, stop, _ = indx.indices(len(self.text))

            if stop < start:
                raise DataValidationError(
                    "Negative length is not allowed, stop < start: "
                    "[%d, %d), text length: %d" % (start, stop, len(self.text)))

            if start > len(self.text) or stop > len(self.text):
                raise DataValidationError("Out of bounds: [%d, %d), "
                                          "text length: %d" % (indx.start, indx.stop, len(self.text)))

            return TextSpan(self, start, stop)
        elif isinstance(indx, tuple) and len(indx) == 2:
            start = int(indx[0])
            stop = int(indx[1])
            if stop < start:
                raise DataValidationError(
                    "Negative length is not allowed, stop < start: "
                    "[%d, %d), text length: %d" % (start, stop, len(self.text)))

            if start < 0 or stop < 0 or start > len(self.text) or stop > len(self.text):
                raise DataValidationError("Out of bounds: [%d, %d), "
                                          "text length: %d" % (start, stop, len(self.text)))

            return TextSpan(self, start, stop)
        elif isinstance(indx, int):
            return self.text[indx]
        else:
            raise ValueError("Unsupported input indx: %s" % repr(indx))


class ExtData:
    """User-defined typed data container"""
    def __init__(self, type, data):
        self.type = type
        self.data = data

    def encode(self):
        if isinstance(self.data, bytes):
            return self.data
        else:
            return bytes(self.data)

    def decode(self):
        return self.data


class DataTypeEnum(Enum):
    """Type names"""
    UNKNOWN = 0  # unsupported for serialization
    I32 = 1
    I64 = 2
    F64 = 3
    BOOL = 4
    STRING = 5
    BINARY = 6
    NODEREF = 7
    NODEREF_MANY = 8
    SPAN = 9
    EXT = 10
    NODEREF_SPAN = 11


# String conversion of enum.
DataType2String = {
    DataTypeEnum.I32: "i32",
    DataTypeEnum.I64: "i64",
    DataTypeEnum.F64: "f64",
    DataTypeEnum.BOOL: "i1",
    DataTypeEnum.STRING: "str",
    DataTypeEnum.BINARY: "bin",
    DataTypeEnum.NODEREF: "noderef",
    DataTypeEnum.NODEREF_MANY: "noderef_array",
    DataTypeEnum.SPAN: "span",
    DataTypeEnum.EXT: "ext",
    DataTypeEnum.NODEREF_SPAN: "nodespan"
}

DataType2PyType = {
    DataTypeEnum.I32: int,
    DataTypeEnum.I64: int,
    DataTypeEnum.F64: float,
    DataTypeEnum.BOOL: bool,
    DataTypeEnum.STRING: str,
    DataTypeEnum.BINARY: bytes,
    DataTypeEnum.NODEREF: Node,
    DataTypeEnum.SPAN: TextSpan,
    DataTypeEnum.EXT: ExtData,
    DataTypeEnum.NODEREF_SPAN: NodeSpan
}

String2DataType = {v: k for k, v in DataType2String.items()}


class DataType:
    """Data type declaration"""
    _type2priority = {DataTypeEnum.BOOL: 0, DataTypeEnum.I32: 1, DataTypeEnum.I64: 2, DataTypeEnum.F64: 3}
    _priority2type = {DataTypeEnum.BOOL: 0, DataTypeEnum.I32: 1, DataTypeEnum.I64: 2, DataTypeEnum.F64: 3}

    def __init__(self, typename: DataTypeEnum, **kwargs):
        self.typename = typename
        self.nativetype = DataType2PyType.get(typename, None)
        self.options = dict(kwargs)

    def default(self):
        return self.options.get("default")

    def encode(self):
        if len(self.options) > 0:
            return {
                "type": DataType2String[self.typename],
                "args": self.options
            }
        else:
            return DataType2String[self.typename]

    def cast_up_possible(self, dtype: "DataType")->bool:
        """Check if type can be merged with another type."""
        if self.typename == dtype.typename:
            return True
        elif self.typename in {DataTypeEnum.BOOL, DataTypeEnum.I32, DataTypeEnum.I64, DataTypeEnum.F64}:
            # BOOL < I32 < I64 < F64
            return dtype.typename in {DataTypeEnum.I32, DataTypeEnum.I64, DataTypeEnum.F64, DataTypeEnum.BOOL}
        else:
            return False

    def cast_up(self, dtype: "DataType")->"DataType":
        """
        Find the largest type capable of representing both.

        :param dtype: type to cast

        :return: self or dtype

        :note:
        String and numbers are not considered being equal.
        """
        assert self.typename in DataType._type2priority
        assert dtype.typename in DataType._type2priority

        if DataType._type2priority[self.typename] >= DataType._type2priority[dtype.typename]:
            return self
        else:
            return dtype

    def is_valid(self, value):
        return True

    def __repr__(self):
        return "DataType(type=%s, options=%s)" % (
            DataType2String.get(self.typename, str(self.typename)),
            repr(self.options)
        )

    def __hash__(self):
        return hash((self.typename, tuple(sorted(self.options.items(), key=lambda x: x[0]))))

    def __eq__(self, dt):
        return self is dt or (self.typename == dt.typename and self.options == dt.options)


class DataTypeBool(DataType):
    def __init__(self, typename: DataTypeEnum, **kwargs):
        super().__init__(typename, **kwargs)

    def is_valid(self, value)->bool:
        return value is not None and isinstance(value, bool)


class DataTypeInt32(DataType):
    def __init__(self, typename: DataTypeEnum, **kwargs):
        super().__init__(typename, **kwargs)

    def is_valid(self, value)->bool:
        return isinstance(value, int) and (-0x80000000 <= value <= 0x7FFFFFFF)


class DataTypeInt64(DataType):
    def __init__(self, typename: DataTypeEnum, **kwargs):
        super().__init__(typename, **kwargs)

    def is_valid(self, value)->bool:
        return isinstance(value, int) and (-0x8000000000000000 <= value <= 0x7FFFFFFFFFFFFFFF)


class DataTypeFloat(DataType):
    def __init__(self, typename: DataTypeEnum, **kwargs):
        super().__init__(typename, **kwargs)

    def is_valid(self, value)->bool:
        return isinstance(value, float)


class DataTypeString(DataType):
    def __init__(self, typename: DataTypeEnum, **kwargs):
        super().__init__(typename, **kwargs)

    def is_valid(self, value)->bool:
        return isinstance(value, str) and len(value) < (2**31)


class DataTypeBinary(DataType):
    def __init__(self, typename: DataTypeEnum, **kwargs):
        super().__init__(typename, **kwargs)

    def is_valid(self, value)->bool:
        return isinstance(value, bytes) and len(value) < (2**31)


class DataTypeNodespan(DataType):
    def __init__(self, typename: DataTypeEnum, **kwargs):
        super().__init__(typename, **kwargs)

    def is_valid(self, value)->bool:
        return isinstance(value, NodeSpan) and \
               value.left.collection is not None and \
               value.right.collection is not None and \
               value.left.collection.name == self.options["layer"] and \
               value.right.collection.name == self.options["layer"] and \
               value.left.i <= value.right.i


class DataTypeTextspan(DataType):
    def __init__(self, typename: DataTypeEnum, **kwargs):
        super().__init__(typename, **kwargs)

    def is_valid(self, value)->bool:
        return isinstance(value, TextSpan) and \
               value.text.name == self.options["context"]


class DataTypeNoderef(DataType):
    def __init__(self, typename: DataTypeEnum, **kwargs):
        super().__init__(typename, **kwargs)

    def is_valid(self, value)->bool:
        return isinstance(value, Node) and \
               value.collection is not None and \
               value.collection.name == self.options["layer"]


class DataTypeNoderefList(DataType):
    def __init__(self, typename: DataTypeEnum, **kwargs):
        super().__init__(typename, **kwargs)

    def is_valid(self, value)->bool:
        target_layer = self.options["layer"]
        return (isinstance(value, list) or isinstance(value, NodeCollection)) and \
               all(map(lambda n: n.collection is not None and n.collection.name == target_layer, value))


class DataTypes:
    """Common datatypes and factory methods for parametrical types"""
    _float64 = DataTypeFloat(DataTypeEnum.F64, default=0)
    _float64_raw = DataTypeFloat(DataTypeEnum.F64)
    _int32 = DataTypeInt32(DataTypeEnum.I32, default=0)
    _int32_raw = DataTypeInt32(DataTypeEnum.I32)
    _int64 = DataTypeInt64(DataTypeEnum.I64, default=0)
    _int64_raw = DataTypeInt64(DataTypeEnum.I64)
    _string = DataTypeString(DataTypeEnum.STRING, default="")
    _string_raw = DataTypeString(DataTypeEnum.STRING)
    _bool = DataTypeBool(DataTypeEnum.BOOL, default=False)
    _bool_raw = DataTypeBool(DataTypeEnum.BOOL)
    binary = DataTypeBinary(DataTypeEnum.BINARY)

    @staticmethod
    def int32(default: Optional[int]=0):
        if default == 0:
            return DataTypes._int32
        elif default is None:
            return DataTypes._int32_raw
        else:
            return DataTypeInt32(DataTypeEnum.I32, default=default)

    @staticmethod
    def int64(default: Optional[int]=0):
        if default == 0:
            return DataTypes._int64
        elif default is None:
            return DataTypes._int64_raw
        else:
            return DataTypeInt64(DataTypeEnum.I64, default=default)

    @staticmethod
    def float64(default: Optional[float]=0.0):
        if default == 0.0:
            return DataTypes._float64
        elif default is None:
            return DataTypes._float64_raw
        else:
            return DataTypeFloat(DataTypeEnum.F64, default=0.0)

    @staticmethod
    def string(default: Optional[str]=""):
        if default == "":
            return DataTypes._string
        elif default is None:
            return DataTypes._string_raw
        else:
            return DataType(DataTypeEnum.STRING, default=default)

    @staticmethod
    def bool(default: Optional[bool]=False):
        if default == "":
            return DataTypes._string
        elif default is None:
            return DataTypes._bool_raw
        else:
            return DataType(DataTypeEnum.BOOL, default=default)

    boolean = bool

    @staticmethod
    def textspan(context: Union[str, Text] = "main"):
        if isinstance(context, Text):
            return context.spantype
        elif isinstance(context, str):
            return DataTypeTextspan(DataTypeEnum.SPAN, context=context)
        else:
            raise ValueError(context)

    span = textspan

    @staticmethod
    def noderef(layer: str):
        return DataTypeNoderef(DataTypeEnum.NODEREF, layer=layer)

    @staticmethod
    def noderef_many(layer: str):
        return DataTypeNoderefList(DataTypeEnum.NODEREF_MANY, layer=layer)

    @staticmethod
    def nodespan(layer: str):
        return DataTypeNodespan(DataTypeEnum.NODEREF_SPAN, layer=layer)

    @staticmethod
    def ext(typename):
        return DataType(DataTypeEnum.EXT, type=typename)

    @staticmethod
    def matches(o, expected):
        return isinstance(o, expected) if expected is not None else None

    @staticmethod
    def typeof(o, comparetype: "DataType"=None) -> DataType:
        if isinstance(o, str):
            return DataTypes.string()
        elif isinstance(o, int):
            if comparetype is not None and comparetype.typename == DataTypeEnum.I32:
                if -0x80000000 <= o <= 0x7FFFFFFF:
                    return DataTypes.int32()
                else:
                    return DataTypes.int64()
            else:
                return DataTypes.int64()
        elif isinstance(o, float):
            return DataTypes.float64()
        elif isinstance(o, bool):
            return DataTypes.bool()
        elif isinstance(o, bytes):
            return DataTypes.binary
        elif isinstance(o, TextSpan):
            return o.text.spantype
        elif isinstance(o, NodeSpan):
            return DataTypes.nodespan(o.left.collection.name)
        elif isinstance(o, Node):
            return o.collection.nodetype
        elif isinstance(o, list):
            if len(o) > 0 and not isinstance(o[0], Node):
                raise ValueError("Unsupported type: %s" % type(o))
            elif len(o) == 0 and comparetype is not None and comparetype.typename == DataTypeEnum.NODEREF_MANY:
                return comparetype
            elif len(o) == 0:  # Assume an empty node list
                return DataType(DataTypeEnum.NODEREF_MANY)
            else:
                layer = o[0].collection
                if sum(1 for n in o if isinstance(n, Node) and n.collection is layer) == len(o):
                    return DataTypes.noderef_many(layer.name)
                else:
                    raise ValueError("Unsupported type: %s" % type(o))
        else:
            raise ValueError("Unsupported type: %s" % type(o))


class NodeLayerSchema:
    """
    Node layer declaration

    Consists of name and field type declarations
    """

    def __init__(self, name: str):
        self.name = name
        self.fields = {}  # type: Dict[str, DataType]

    def add(self, name: str, fieldtype: Union[Callable, "DataType"]):
        if name in self.fields:
            raise ValueError("Field '%s' already exists on layer %s" % (name, self.name))

        fieldtype = fieldtype() if callable(fieldtype) else fieldtype
        assert isinstance(fieldtype, DataType), "Type of field '%s' is not a DataType, it is: %s" % \
                                                (name, repr(fieldtype))
        self.fields[name] = fieldtype
        return self

    def set(self, **kwargs):
        for k, v in kwargs.items():
            v = v() if callable(v) else v
            assert isinstance(v, DataType), "Type of field '%s' is not a DataType, it is: %s" % (k, repr(v))

            if k in self.fields:
                raise ValueError("Field '%s' already exists on layer %s" % (k, self.name))

            self.fields[k] = v

        return self


class NodeCollectionQuery(NodeCollection):
    """Represents a query to document data"""
    def __init__(self, collection: "NodeCollection", predicate: Callable[["Node"], bool]):
        NodeCollection.__init__(self, fieldtypes=collection.fieldtypes)
        self.collection = collection
        self.predicate = predicate
        self.result = [n for n in collection if predicate(n)]

    def __iter__(self):
        return iter(self.result)

    def __len__(self):
        return len(self.result)

    def __getitem__(self, item):
        if isinstance(item, str):
            if item in self.collection.schema.fields:
                return NodeFieldCollection(self, item)
        else:
            return super().__getitem__(item)

    def update(self):
        self.result = [n for n in self.collection if self.predicate(n)]

    def __repr__(self):
        return "NodeCollectionQuery(collection=%s, N=%d)" % (self.collection.name, len(self))

    def _repr_html_(self):
        return self._table_repr_("Query with %d nodes.").render_html()

    def __str__(self):
        return self._table_repr_("Query with %d nodes.").render_text()


class NodeLayerCollection(NodeCollection):
    """Node collection, internally a list with gaps which will compact when 25% of the list is empty."""
    def __init__(self, schema: "NodeLayerSchema"):
        super().__init__(schema.fields)
        self.nodetype = DataTypes.noderef(schema.name)
        self._schema = schema
        self._nodes = []  # type: List[Node]
        self.num = 0
        self._default_values = {}  # type: Dict[str, Any]
        self._update_default_values()

    @property
    def schema(self)->"NodeLayerSchema":
        """Get layer schema"""
        return self._schema

    @property
    def name(self)->str:
        """Name of layer"""
        return self.schema.name

    def _update_default_values(self):
        self._default_values = {field: typedef.default() for field, typedef in self.schema.fields.items() if
                                typedef.default() is not None}

    def add_field(self, name: str, type: "DataType", init_with_default=True):
        """
        Add new field to the schema

        :param name: name of the field
        :param type: type of the field
        :param init_with_default: set all existing nodes fields to default value

        :raises SchemaValidationError if the field conflicts with existing field
        """
        if name in self._schema.fields:
            raise SchemaValidationError("Cannot add field %s, it already exists!" % name)

        self._schema.add(name, type)
        self._update_default_values()

        # Set all current values to the default
        defaultvalue = type.default()
        for n in self:
            n[name] = defaultvalue

    def remove_field(self, name: str, leave_data=False)->bool:
        """
        Remove existing field

        :param name: the name of the field to remove
        :param leave_data: leave any existing data in nodes, validation fails with default settings if not cleaned out.

        :return: true if the field was remove, false if the field could not be found
        """
        if name not in self._schema.fields:
            return False

        if not leave_data:
            for n in self:
                if name in n:
                    del n[name]

        del self._schema.fields[name]
        self._update_default_values()
        return True

    def unsafe_initialize(self, nodes: List[Node])->"NodeLayerCollection":
        """
        Directly replaces all nodes with the provided list, no checks for performance.

        :note:
        **Unsafe**, used for direct initialization by codecs.

        :return: self
        """

        self.num = len(nodes)
        self._nodes = nodes
        return self

    def validate(self, node: "Node")->bool:
        """Validate node against schema, will throw SchemaTypeError if not valid."""
        for field, fieldtype in self.schema.fields.items():
            if field in node:
                fieldvalue = node[field]
                if not node.collection.fieldtypes[field].is_valid(fieldvalue):
                    if fieldtype.typename == DataTypeEnum.NODEREF_SPAN:
                        assert fieldvalue.left.collection is not None, \
                            "Left node is removed in nodespan: " \
                            "field '%s' in layer '%s' for node %s" % (field, self.name, repr(node))
                        assert fieldvalue.right.collection is not None, \
                            "Right node is removed in nodespan: " \
                            "field '%s' in layer '%s' for node %s" % (field, self.name, repr(node))
                        assert fieldvalue.left.collection.name == fieldtype.options["layer"], \
                            "Left node does not match layer %s: " \
                            "field '%s' in layer '%s' for node %s" \
                            % (fieldtype.options["layer"], field, self.name, repr(node))
                        assert fieldvalue.right.collection.name == fieldtype.options["layer"], \
                            "Right node does not match layer %s: " \
                            "field '%s' in layer '%s' for node %s" \
                            % (fieldtype.options["layer"], field, self.name, repr(node))
                        assert fieldvalue.left.i <= fieldvalue.right.i, \
                            "Ordering is for this nodespan is invalid (%d, %d): " \
                            "field '%s' in layer '%s' for node %s" % \
                            (fieldvalue.left.i, fieldvalue.right.i, field, self.name, repr(node))
                    elif fieldtype.typename == DataTypeEnum.NODEREF:
                        assert fieldvalue.collection is not None, \
                            "Node is removed: " \
                            "field '%s' in layer '%s' for node %s" % (field, self.name, repr(node))
                        assert fieldvalue.collection.name == fieldtype.options["layer"], \
                            "Node does not match layer %s: " \
                            "field '%s' in layer '%s' for node %s" \
                            % (fieldtype.options["layer"], field, self.name, repr(node))
                    elif fieldtype.typename == DataTypeEnum.NODEREF_MANY:
                        assert isinstance(fieldvalue, list), "Not a node list: " \
                                "field '%s' in layer '%s' for node %s" % (field, self.name, repr(node))
                        for n in fieldvalue:
                            assert isinstance(n, Node), "Not a node: " \
                                "field '%s' in layer '%s' for node %s" % (field, self.name, repr(node))
                            assert n.collection is not None, \
                                "Node is removed: " \
                                "field '%s' in layer '%s' for node %s" % (field, self.name, repr(node))
                            assert n.collection.name == fieldtype.options["layer"], \
                                "Node does not match layer %s: " \
                                "field '%s' in layer '%s' for node %s" \
                                % (fieldtype.options["layer"], field, self.name, repr(node))
                    elif fieldtype.typename == DataTypeEnum.SPAN:
                        assert isinstance(fieldvalue, TextSpan), \
                            "The span field '%s' was not set to a TextSpan, but: '%s'" % \
                            (field, repr(fieldvalue))

                        assert fieldvalue.text.name == fieldtype.options["context"], \
                            "Textspan does not match expected context %s found %s: " \
                            "field '%s' in layer '%s' for node %s" % \
                            (fieldtype.options["context"], fieldvalue.text.name, field, self.name, repr(node))
                    else:
                        assert False, \
                        "Invalid value in field %s, typeof(%s) does not match %s. Ref: %s" % \
                        (field, repr(node[field]), repr(fieldtype), repr(node))

                    return False

        return True

    def add(self, *args, **kwargs) -> Node:
        """
        Add node to this layer.

        :param args: Node objects, if used then kwargs are ignored
        :param kwargs: create nodes from given properties, ignored if len(args) > 0

        :return: node if kwargs was used

        :Example:
        >>> layer = doc["layer-name"]  # type: NodeLayerCollection
        >>> layer.add(field1="Data", field2=42, field3=text[0:12])
        >>> layer.add(node1, node2)
        >>> layer.add(*nodes)
        """
        if len(args) > 0:
            for n in args:
                if isinstance(n, Node):
                    n._id = len(self._nodes)
                    n.collection = self
                    self._nodes.append(n)
                    self.num += 1
                else:
                    raise ValueError(n)
        else:
            node = Node(self._default_values)
            node.update(**kwargs)
            node._id = len(self._nodes)
            node.collection = self
            assert self.validate(node), "Node not valid."
            self._nodes.append(node)
            self.num += 1
            return node

    def add_many(self, nodes: Iterable["Node"], default_fill=True, full_validation=True):
        """
        Add many nodes

        :param nodes: list of node references to add
        :param default_fill: set to True if default values should be added to nodes
        :param full_validation: set to True to do full field validation

        :note:
        If full_validation is set to True, it will first add all nodes, and then perform validation. \
        Internal references between nodes in the nodes input is allowed.
        """
        start_pos = len(self._nodes)
        for node in nodes:
            assert isinstance(node, Node), "Got a node which is not a Node: %s" % repr(node)
            assert node.collection is None, "Node is already bound to a collection: %s" % repr(node.collection)

            node._id = len(self._nodes)
            node.collection = self

            self._nodes.append(node)
            self.num += 1

        if full_validation:
            for node in nodes:
                assert self.validate(node), "Node not valid"

        if default_fill and len(self._default_values) > 0:
            keys = set(self._default_values.keys())
            for n in self._nodes[start_pos:]:
                for missing_key in keys.difference(n.keys()):
                    n[missing_key] = self._default_values[missing_key]

    def compact(self):
        """
        Compact this layer to have no gaps.

        All node references will be stored sequentially in memory.
        """
        if len(self._nodes) == self.num:
            # No compacting needed.
            return

        i = 0
        for k in range(0, len(self._nodes)):
            if self._nodes[k] is not None:
                self._nodes[i] = self._nodes[k]
                self._nodes[i]._id = i
                i += 1

        # Trim it
        del self._nodes[i:]

    def filter(self, *fields, fn):
        """
        Create a node filter predicate

        :param fields: the fields for the predicate
        :param pred: callable object which given values will return true/false
        """
        return NodeLambdaPredicate(fn, fields)

    def sort(self, keyfn):
        """
        Sort the nodes, rearrange the node reference order by the given key function

        :param keyfn: a function (input: Node) -> value to sort by.
        """
        self.compact()
        self._nodes.sort(key=keyfn)
        for i, n in zip(range(len(self)), self):
            n._id = i

    def remove(self, node: Union["Node", Iterable["Node"]]):
        """
        Remove nodes

        :param node: the node or list of nodes to remove
        """
        if isinstance(node, Node):
            if node.collection is not self:
                raise ValueError("Node %s is not in this node collection %s" % (repr(node), self.name))

            self._nodes[node._id] = None
            self.num -= 1
            node.collection = None
        else:
            # Attempt to get an iterable from input
            try:
                for n in iter(node):
                    if isinstance(n, Node):
                        if n.collection is not self:
                            raise ValueError("Node %s is not in this node collection %s" % (repr(node), self.name))

                        self._nodes[n._id] = None
                        self.num -= 1
                        n.collection = None
                    elif n is None:
                        pass
                    else:
                        raise ValueError("Unsupported object was requested to be "
                                         "removed from this collection: %s " % (repr(n)))

            except TypeError:
                raise ValueError(
                    "Unsupported object was requested to be removed from this collection: %s " % (repr(node)))

        if 0.75*len(self._nodes) > self.num and len(self._nodes) > 16:
            self.compact()

    def retain(self, nodes: Iterable["Node"]):
        """Retain all nodes in the given list nodes, remove everything else."""

        try:
            for n in iter(nodes):
                # Mark the nodes to retain
                if n.collection is not self:
                    raise ValueError("Node %s is not in this node collection '%s'" % (repr(n), self.name))

                # Guards against more than one reference to a node in nodes
                if n._id >= 0:
                    n._id = -(n._id + 1)  # add 1, otherwise zero fails.

        except TypeError:
            raise ValueError("Given nodes is not iterable.")

        # Compact and remove unmarked nodes
        k = 0
        for i in range(len(self._nodes)):
            n = self._nodes[i]
            if n._id >= 0:
                n.collection = None
                self.num -= 1
            else:
                self._nodes[k] = n
                n._id = k
                k += 1

        # Compact list
        del self._nodes[k:]

    def left(self, n: "Node")->Optional["Node"]:
        """:return: node to the left or lower index than given n or None if none available."""
        assert n.collection is self, "Node is not in this collection"

        for i in range(n.i-1, -1, -1):
            if self._nodes[i] is not None:
                return self._nodes[i]

        return None

    def right(self, n: "Node")->Optional["Node"]:
        """:return: node to the right or larger index than given n or None if none available."""
        assert n.collection is self, "Node is not in this collection"

        for i in range(n.i+1, len(self._nodes)):
            if self._nodes[i] is not None:
                return self._nodes[i]

        return None

    def iter_nodespan(self, left_most: "Node", right_most: "Node")->Iterator["Node"]:
        """
        Iterator for node in given span

        :param left_most: left most, lowest index node
        :param right_most: right most, highest index node, inclusive.

        :return: iterator yielding zero or more elements
        """
        assert left_most.i <= right_most.i, "Nodes not in order"
        assert left_most.collection == right_most.collection and right_most.collection is self, \
            "Collection did not match"

        return filter(None.__ne__, map(self._nodes.__getitem__, range(left_most.i, right_most.i+1)))

    def __iter__(self)->Iterator[Node]:
        if self.num == len(self._nodes):
            return iter(self._nodes)
        else:
            return filter(None.__ne__, self._nodes)

    def __repr__(self):
        return "Layer(%s, N=%d)" % (self.name, self.num)

    def _repr_html_(self):
        return self._table_repr_("Layer '%s' with %%d nodes." % self.name).render_html()

    def __str__(self):
        return self._table_repr_("Layer '%s' with %%d nodes." % self.name).render_text()

    def __len__(self):
        return self.num

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._nodes[item]
        elif isinstance(item, Node):
            if item.collection is self:
                return item
            else:
                raise DataValidationError("Node is not part of this node collection '%s': %s" % (self.name, repr(item)))
        elif isinstance(item, slice):
            return NodeList(iter(n for n in filter(None.__ne__, self._nodes[item])), fieldtypes=self._schema.fields)
        else:
            return super().__getitem__(item)

    def _ipython_key_completions_(self):
        return list(self.schema.fields.keys())

    def __delitem__(self, node: Union["Node", Iterable["Node"]]):
        self.remove(node)

    def first(self):
        return next(filter(None.__ne__, self._nodes), None)

    def last(self):
        return next(filter(None.__ne__, map(self._nodes.__getitem__, range(len(self._nodes)-1, -1, -1))), None)

    def to_pandas(self, fields: List[str]=None, materialize_spans=False, include_ref_field=True):
        """
        Convert this layer to a pandas Dataframe

        Requires Pandas which is not a requirement for Docria.

        :param fields:  which fields to include, by default all fields are included.
        :param materialize_spans: converts span fields to a materialized string
        :param include_ref_field: include the python node reference as __ref field in the dataframe.

        :rtype: pandas.DataFrame
        :return: pandas.Dataframe with the contents of this layer
        """
        from pandas import DataFrame

        if fields is None:
            fields = self.schema.fields.items()
        else:
            fields = [(field, self.schema.fields[field]) for field in fields]

        if materialize_spans:
            data = {}
            for field, fieldtype in fields:
                if fieldtype.typename == DataTypeEnum.SPAN:
                    data[field] = [(str(n[field]) if field in n else None) for n in filter(None.__ne__, self._nodes)]
                else:
                    data[field] = [n.get(field) for n in filter(None.__ne__, self._nodes)]
        else:
            data = {k: [n.get(k) for n in filter(None.__ne__, self._nodes)] for k, v in fields}

        if include_ref_field:
            data["__ref"] = list(filter(None.__ne__, self._nodes))

        return DataFrame(data=data)

    def __contains__(self, item: Node):
        return item.collection is self


class Document:
    """The document which contains all data

    .. automethod:: __getitem__
    .. automethod:: __delitem__
    .. automethod:: __contains__
    """
    def __init__(self, **kwargs):
        """
        Construct new document

        :param kwargs: property key, values
        """
        self._layers = {}  # type: Dict[str, NodeLayerCollection]
        self._texts = {}  # type: Dict[str, Text]
        self.props = dict(kwargs)  # type: Dict[str, Any]

    @property
    def text(self)->Dict[str, Text]:
        """Text"""
        return self._texts

    @property
    def texts(self)->Dict[str, Text]:
        """Alias for :meth:`~docria.Document.text`"""
        return self._texts

    @property
    def layer(self)->Dict[str, NodeLayerCollection]:
        """Layer dict"""
        return self._layers

    @property
    def layers(self)->Dict[str, NodeLayerCollection]:
        """Alias for :meth:`~docria.Document.layer`"""
        return self._layers

    @property
    def maintext(self)->Text:
        return self._texts["main"]

    @maintext.setter
    def maintext(self, text: Union[str, Text]):
        assert "main" not in self._texts, "It is not allowed to replace main as it may affect layers referencing it."

        if isinstance(text, str):
            maintext = Text("main", text)
            self._texts["main"] = maintext
        elif isinstance(text, Text):
            assert text.name == "main", "Text is not named main"
            self._texts["main"] = text
        else:
            raise ValueError("text is of unknown type: %s" % repr(type(text)))

    def add_text(self, name, text):
        """
        Add text to the document

        :param name: name of the context
        :param text: the raw string
        :return: Text instance that can be used to derive spans form
        """
        txtobj = Text(name, text)
        self.texts[name] = txtobj
        return txtobj

    def add_layer(self, __name: Union[str, NodeLayerSchema], **kwargs):
        """
        Create and add layer with specified schema

        :param __name: the name of the layer
        :param kwargs: key value pairs with e.g. name of field = type of field
        :return: NodeLayerCollection instance with the specified schema
        """
        if isinstance(__name, NodeLayerSchema):
            typedef = __name
        else:
            typedef = NodeLayerSchema(__name)
            typedef.set(**kwargs)

        typecol = NodeLayerCollection(typedef)
        self.layers[typedef.name] = typecol
        return typecol

    def remove_layer(self, name, fieldcascade=False)->bool:
        """
        Remove layer from document if it exists.

        :param name: name of layer
        :param fieldcascade: force removal, and cascade removal of referring fields in other layers,
                             default: false which will result in exception if any layer is referring to name
        :return: True if layer was removed, False if it does not exist
        """
        if name not in self.layers:
            return False

        referencing_layer_field = {}
        for k, v in self.layers.items():
            if k != name:
                for fk, fv in v.schema.fields.items():
                    if fv.typename == DataTypeEnum.NODEREF or fv.typename == DataTypeEnum.NODEREF_MANY:
                        if fv.options["layer"] == name:
                            referencing_layer_field.setdefault(k, []).append(fk)

        if not fieldcascade and len(referencing_layer_field) > 0:
            layer_field_names = ", ".join(
                map(lambda tup: "%s(%s)" % (tup[0], ", ".join(tup[1]))
                    , referencing_layer_field.items())
            )

            raise DataValidationError("Attempting to remove layer %s, but is referenced from layer(s)+field(s): %s"
                                        % (name, layer_field_names))

        del self.layers[name]

    def __repr__(self):
        return "Document(%d layers, %d texts%s)" % (
            len(self.layers),
            len(self.texts),
            (", " + ", ".join(map(lambda tup: "%s=%s" % tup, self.props.items()))) if len(self.props) > 0 else ""
        )

    def __str__(self):
        from docria.printout import Table, options
        output = ["== Document =="]
        tbl_texts = Table(caption="Texts", hide_headers=True, hide_index=True)
        tbl_texts.set_header("key", "value")

        for k, v in self.texts.items():
            tbl_texts.add_body(k, repr(v.text))

        output.append(tbl_texts.render_text())

        tbl_layers = Table(caption="Layers", hide_headers=True, hide_index=True)
        tbl_layers.set_header("key", "value")
        for k, v in self.layers.items():
            tbl_layers.add_body(k, "N={:}".format(len(v)))

        output.append(tbl_layers.render_text())

        return "\n".join(output)

    def printschema(self):
        """Prints the full schema of this document to stdout, containing layer fields and typing information"""
        for k, v in sorted(self.layers.items(), key=lambda tup: tup[0]):
            print("[%s]" % k)
            max_length = max(map(len, v.schema.fields.keys()), default=0)

            for field, fieldtype in v.schema.fields.items():
                print((" * {:<%d} : {:}{:}" % max_length).format(
                    field,
                    DataType2String[fieldtype.typename],
                    "" if len(fieldtype.options) == 0
                    else "[%s]" % ", ".join(map(lambda tup: "%s=%s" % tup, fieldtype.options.items()))
                ))
            print()

    def __getstate__(self):
        from docria.codec import MsgpackCodec

        return {
            "msgpacked": MsgpackCodec.encode(self)
        }

    def __setstate__(self, state):
        from docria.codec import MsgpackCodec

        doc = MsgpackCodec.decode(state["msgpacked"])
        self._layers = doc.layers
        self._texts = doc.texts
        self.props = doc.props

    def __getitem__(self, key):
        return self.layers[key]

    def _ipython_key_completions_(self):
        return list(self.layers.keys())

    def __delitem__(self, key):
        return self.remove_layer(name=key)

    def __contains__(self, item):
        return item in self._layers

    def compile(self, extra_fields_ok=False, type_validation=True, **kwargs)->\
            Dict[str, Tuple[Dict[int, int], List[int]]]:
        """Compile the document, validates and assigns compacted ids to nodes (internal use)

        :param extra_fields_ok: ignores extra fields in node if set to True
        :param type_validation: do type validation, if set to False and type
                                is not correct will result in undefined behaviour, possibly corrupt storage.

        :returns: Dictionary of text id to Dict(offset, offset-id)
        :raises SchemaValidationError:
        """

        # Extract layer and text references
        referenced_layers = set()
        referenced_texts = set()
        for layer in self.layers.values():
            for field, fieldtype in layer.schema.fields.items():
                if fieldtype.typename in {DataTypeEnum.NODEREF, DataTypeEnum.NODEREF_MANY, DataTypeEnum.NODEREF_SPAN}:
                    referenced_layers.add((fieldtype.options["layer"], (layer.name, field)))
                elif fieldtype.typename == DataTypeEnum.SPAN:
                    referenced_texts.add((fieldtype.options["context"], (layer.name, field)))

        # Verify referenced layers
        for layer, (src_layer, src_field) in referenced_layers:
            if layer not in self.layers:
                raise SchemaError("Layer %s could not be found, "
                                  "it was referenced by layer '%s' and field '%s'." % (layer, src_layer, src_field))

        # Verify referenced texts
        for text, (src_layer, src_field) in referenced_texts:
            if text not in self.texts:
                raise SchemaError("Text with context '%s' could not be found, "
                                  "it was referenced by layer '%s' and field '%s'." % (text, src_layer, src_field))

        # Assign node ids and validate nodes
        text_offsets = {k: set() for k, _ in self._texts.items()}

        for k, v in self.layers.items():
            for idref, n in zip(range(len(v)), v):
                n._id = idref

            fieldtypes = v.schema.fields
            fieldkeys = set(fieldtypes.keys())

            v.compact()
            validate_fn = v.validate

            # Validate nodes
            if type_validation and not extra_fields_ok:
                for n in v:
                    validate_fn(n)

                    if not extra_fields_ok and len(set(n.keys()).difference(fieldkeys)) > 0:
                        raise SchemaValidationError(
                            "Extra fields not declared in schema was found for layer %s, fields: %s" % (
                                k, ", ".join(set(n.keys()).difference(fieldkeys))), set(n.keys())
                        )

            # Collect span offsets
            for field, fieldtype in fieldtypes.items():
                if fieldtype.typename == DataTypeEnum.SPAN:
                    offsets = text_offsets[fieldtype.options["context"]]
                    offset_add = offsets.add
                    for n in v:
                        if field in n:
                            # Add offsets
                            fieldvalue = n[field]
                            offset_add(fieldvalue.start)
                            offset_add(fieldvalue.stop)

        text_offset_mapping = {}
        for k, v in self._texts.items():
            offsets = text_offsets[k]
            offsets.add(0)
            offsets.add(len(v.text))

            sorted_offsets = sorted(offsets)
            text_offset_mapping[k] = ({k: v for k, v in zip(sorted_offsets, range(len(offsets)))}, sorted_offsets)

        return text_offset_mapping
