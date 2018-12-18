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

from typing import Dict, List, Tuple, Callable, Any, Iterator, Iterable, Union
from enum import Enum


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


class DictAsMembers:
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
    """Basic building block of the document model"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._id = -1  # type: int
        self.collection = None  # type: NodeLayerCollection

    @property
    def fld(self):
        """Get a pythonic wrapper for this node .e.g node.fld.id == node["id"] """
        return DictAsMembers(self)

    def detach(self):
        """Remove itself from the document model"""
        self.collection.remove(self)

    def is_dangling(self):
        """Check if this node is dangling i.e. is not attached to an existing layer, possibly removed or never added."""
        return self.collection is None

    def validate(self):
        self.collection.validate(self)

    def _ipython_key_completions_(self):
        return list(self.keys())

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return self is other

    def __str__(self):
        tbl = self._table_repr_()
        return tbl.render_text()

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


class NodeList(list):
    def __init__(self, iterable: Iterable[Node]):
        super().__init__(iterable)

    def __iter__(self)->Iterator[Node]:
        return super().__iter__()

    def _table_repr_(self):
        fields = set()

        for n in self:
            fields.update(n.keys())

        fields = sorted(fields)
        from docria.printout import Table, TableRow, get_representation
        tbl = Table("Node list with N=%d elements" % len(self))

        tbl.set_header(*fields)
        for n in self:
            values = list(map(lambda k: get_representation(n.get(k, None)), fields))
            tbl.add_body(TableRow(*values))

        return tbl

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
    """Text span, consisting of a start and stop offset.

       Remarks: Use str(span) to get a real string.
    """
    def __init__(self, text: "Text", start_offset: "Offset", stop_offset: "Offset"):
        self.text = text
        self.start_offset = start_offset
        self.stop_offset = stop_offset

    @property
    def start(self)->int:
        return int(self.start_offset)

    @property
    def stop(self)->int:
        return int(self.stop_offset)

    def __len__(self):
        return int(self.stop_offset)-int(self.start_offset)

    def __hash__(self):
        return hash((self.start_offset, self.stop_offset))

    def __eq__(self, textrange):
        return (self.start_offset, self.stop_offset) == (textrange.startOffset, textrange.stopOffset)

    def __getitem__(self, indx: slice):
        if indx.step is not None and indx.step != 1:
            raise NotImplementedError("Only step == 1 are supported.")

        start, stop, step = indx.indices(len(self))
        return self.text[start+self.start:stop+self.start]

    def __repr__(self):
        return "span(%s[%d:%d]) = %s" % (
            self.text.name,
            self.start_offset.offset,
            self.stop_offset.offset,
            repr(self.text.text[self.start_offset.offset:self.stop_offset.offset])
        )

    def __str__(self):
        return self.text.text[self.start_offset.offset:self.stop_offset.offset]


class Text:
    """Text object, consisting of text and an index of current offsets"""
    def __init__(self, name, text):
        self.name = name
        self.text = text
        self.spantype = DataType(TEnum.SPAN, context=self.name)
        self._offsets = {}  # type: Dict[int, Offset]

    def reset_counter(self):
        """
        Resets the reference counter.
        """
        for v in self._offsets.values():
            v._refcnt = 0

    def initialize_offsets(self, offsets: List[int])->List[Offset]:
        """
        Unsafe direct initialization of offset objects, used by codecs.

        :param offsets: the offset list
        :return: list of offset objects
        """
        offset_objs = []  # type: List[Offset]
        for off in offsets:
            offobj = Offset(off)
            self._offsets[off] = offobj
            offset_objs.append(offobj)

        if len(offset_objs) == 0 or offset_objs[-1].offset != len(self.text):
            offobj = Offset(len(self.text))
            self._offsets[offobj.offset] = offobj
            offset_objs.append(offobj)

        return offset_objs

    def __str__(self):
        """Convert to string"""
        return self.text

    def _repr_html_(self):
        from html import escape
        return "<h3>Text: {0}</h3><pre>{1}</pre>".format(self.name, escape(self.text))

    def _gc(self):
        """Remove unused offsets. Assumes reference counter has been properly initialized."""
        self._offsets = {k: v for k, v in self._offsets.items() if v._refcnt > 0}

    def _compile(self):
        """
        Compiles text objects for serialization, sets the offset _id field to match segment ids.
        :return: List of segments
        """

        for removeoff in [off for off in self._offsets.values() if off._refcnt == 0]:
            del self._offsets[removeoff.offset]

        if 0 not in self._offsets:
            self._offsets[0] = Offset(0)

        if len(self.text) not in self._offsets:
            self._offsets[len(self.text)] = Offset(len(self.text))

        offsets = sorted(self._offsets.values(), key=lambda off: off.offset)

        output = []
        for i in range(len(offsets)-1):
            start = offsets[i]
            stop = offsets[i+1]

            start._id = i
            output.append(self.text[start.offset:stop.offset])

        offsets[-1]._id = len(offsets)-1

        return output

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

            start_offset = self._offsets.setdefault(start, Offset(indx.start))
            stop_offset = self._offsets.setdefault(stop, Offset(indx.stop))
            return TextSpan(self, start_offset, stop_offset)
        elif isinstance(indx, tuple) and len(indx) == 2:
            start_offset = self._offsets.setdefault(int(indx[0]), Offset(int(indx[0])))
            stop_offset = self._offsets.setdefault(int(indx[1]), Offset(int(indx[1])))
            return TextSpan(self, start_offset, stop_offset)
        else:
            raise ValueError("Unsupported input indx: %s" % repr(indx))


class ExtData:
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


class TEnum(Enum):
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

# String conversion of enum.
DataType2String = {
    TEnum.I32: "i32",
    TEnum.I64: "i64",
    TEnum.F64: "f64",
    TEnum.BOOL: "i1",
    TEnum.STRING: "str",
    TEnum.BINARY: "bin",
    TEnum.NODEREF: "noderef",
    TEnum.NODEREF_MANY: "noderef_array",
    TEnum.SPAN: "span",
    TEnum.EXT: "ext"
}

DataType2PyType = {
    TEnum.I32: int,
    TEnum.I64: int,
    TEnum.F64: float,
    TEnum.BOOL: bool,
    TEnum.STRING: str,
    TEnum.BINARY: bytes,
    TEnum.NODEREF: Node,
    TEnum.SPAN: TextSpan,
    TEnum.EXT: ExtData
}

String2DataType = {v: k for k, v in DataType2String.items()}


class DataType:
    """Data type declaration"""
    def __init__(self, typename: TEnum, **kwargs):
        self.typename = typename
        self.nativetype = DataType2PyType.get(typename, None)
        self.options = dict(kwargs)

    def encode(self):
        if len(self.options) > 0:
            return {
                "type": DataType2String[self.typename],
                "args": self.options
            }
        else:
            return DataType2String[self.typename]

    def __repr__(self):
        return "DataType(type=%s, options=%s)" % (
            DataType2String.get(self.typename, str(self.typename)),
            repr(self.options)
        )

    def __hash__(self):
        return hash((self.typename, tuple(sorted(self.options.items(), key=lambda x: x[0]))))

    def __eq__(self, dt):
        return self is dt or (self.typename == dt.typename and self.options == dt.options)


class DataTypes:
    """Common datatypes and factory methods for parametrical types"""
    float64 = DataType(TEnum.F64)
    int32 = DataType(TEnum.I32)
    int64 = DataType(TEnum.I64)
    string = DataType(TEnum.STRING)
    binary = DataType(TEnum.BINARY)
    bool = DataType(TEnum.BOOL)

    @staticmethod
    def span(context):
        return DataType(TEnum.SPAN, context=context)

    @staticmethod
    def noderef(layer):
        return DataType(TEnum.NODEREF, layer=layer)

    @staticmethod
    def noderef_many(layer):
        return DataType(TEnum.NODEREF_MANY, layer=layer)

    @staticmethod
    def ext(typename):
        return DataType(TEnum.EXT, type=typename)

    @staticmethod
    def matches(o, expected):
        return isinstance(o, expected) if expected is not None else None

    @staticmethod
    def typeof(o, comparetype: "DataType"=None, fasttype=None) -> DataType:
        if isinstance(o, str):
            return DataTypes.string
        elif isinstance(o, int):
            if comparetype is not None and comparetype.typename == TEnum.I32:
                if -0x80000000 <= o <= 0x7FFFFFFF:
                    return DataTypes.int32
                else:
                    return DataTypes.int64
            else:
                return DataTypes.int64
        elif isinstance(o, float):
            return DataTypes.float64
        elif isinstance(o, bool):
            return DataTypes.bool
        elif isinstance(o, bytes):
            return DataTypes.binary
        elif isinstance(o, TextSpan):
            return o.text.spantype
        elif isinstance(o, Node):
            return o.collection.nodetype
        elif isinstance(o, list):
            if len(o) > 0 and not isinstance(o[0], Node):
                raise ValueError("Unsupported type: %s" % type(o))
            elif len(o) == 0 and comparetype is not None and comparetype.typename == TEnum.NODEREF_MANY:
                return comparetype
            elif len(o) == 0:  # Is invalid in any case as the comparetype is unknown or not a list type.
                return DataType(TEnum.UNKNOWN)
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

    def add(self, name: str, fieldtype: "DataType"):
        if name in self.fields:
            raise ValueError("Field '%s' already exists on layer %s" % (name, self.name))

        self.fields[name] = fieldtype
        return self

    def set(self, **kwargs):
        for k, v in kwargs.items():
            assert isinstance(v, DataType), "Type of field '%s' is not a DataType, it is: %s" % (k, repr(v))

            if k in self.fields:
                raise ValueError("Field '%s' already exists on layer %s" % (k, self.name))

            self.fields[k] = v

        return self


class NodeLayerCollection:
    """
    Node collection, internally a skip-list which will compact when 25% of the list is empty.
    """
    def __init__(self, schema: "NodeLayerSchema"):
        self.nodetype = DataTypes.noderef(schema.name)
        self._schema = schema
        self._nodes = []
        self.num = 0

    @property
    def schema(self):
        return self._schema

    @property
    def name(self):
        return self.schema.name

    def add_field(self, name: str, type: "DataType"):
        if name in self._schema.fields:
            raise SchemaValidationError("Cannot add field %s, it already exists!" % name)

        self._schema.add(name, type)

    def remove_field(self, name: str)->bool:
        if name not in self._schema:
            return False

        for n in self:
            if name in n:
                del n[name]

        del self._schema.fields[name]
        return True

    def unsafe_initialize(self, nodes: List[Node]):
        """Directly replaces all nodes with the provided list, no checks for performance.

           Remarks: Only use this method if you know what you are doing!"""

        self.num = len(nodes)
        self._nodes = nodes
        for node_id, node in zip(range(len(self._nodes)), self._nodes):
            node._id = node_id
            node.collection = self

    def validate(self, node: "Node"):
        """Validate node against schema, will throw SchemaTypeError if not valid."""
        for field, fieldtype in self.schema.fields.items():
            if field in node:
                if DataTypes.matches(node[field], fieldtype.nativetype):
                    continue
                else:
                    try:
                        valuetype = DataTypes.typeof(node[field], fieldtype)
                        if valuetype != fieldtype:
                            raise DataValidationError("Invalid node, typeof(%s) = %s does not match %s. Ref: %s" % (
                                repr(node[field]), repr(valuetype), repr(fieldtype), repr(node)))
                    except ValueError as e:
                        raise DataValidationError("Found value which was not supported: "
                                                  "%s, in layer %s, field %s, node with id: %d" %
                                                  (node[field], node.collection.name, field, node._id)) from e

        return True

    def add(self, *args, **kwargs) -> Node:
        """
        Add node to this layer

        :param args: Node objects, if used then kwargs are ignored
        :param kwargs: create nodes from given properties, ignored if len(args) > 0
        :return: node if kwargs was used
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
            node = Node(**kwargs)
            node._id = len(self._nodes)
            node.collection = self
            self._nodes.append(node)
            self.num += 1
            return node

    def add_many(self, nodes):
        num_nodes = self.num
        for idval, node in zip(range(num_nodes, num_nodes+len(nodes)), nodes):
            node._id = idval
            node.collection = self

        self.num += len(nodes)
        self._nodes.extend(nodes)

    def compact(self):
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

    def sort(self, keyfn):
        self.compact()
        self._nodes.sort(key=keyfn)
        for i, n in zip(range(len(self)),self):
            n._id = i

    def remove(self, node: "Node"):
        if node.collection is not self:
            raise ValueError("Node %s is not in this node collection %s" % (repr(node), self.name))

        self._nodes[node._id] = None
        self.num -= 1

        if 0.75*len(self._nodes) > self.num and len(self._nodes) > 16:
            self.compact()
        
        node.collection = None

    def __iter__(self)->Iterator[Node]:
        if self.num == len(self._nodes):
            return iter(self._nodes)
        else:
            def valueiter():
                for node in self._nodes:
                    if node is None:
                        continue

                    yield node

            return valueiter()

    def __repr__(self):
        return "Layer(%s, N=%d)" % (self.name, self.num)

    def _table_repr_(self):
        from docria.printout import Table, TableRow, get_representation

        cols = list(sorted(self.schema.fields.keys()))
        fields = list(sorted(self.schema.fields.keys()))

        table = Table(caption="Layer: %s" % self.name)
        table.set_header(*cols)

        for n in self:
            fld_data = [n.get(fld, None) for fld in fields]
            for i in range(len(fld_data)):
                fld_data[i] = get_representation(fld_data[i])

            table.add_body(TableRow(*fld_data, index="#%d" % n._id))

        return table

    def _repr_html_(self):
        return self._table_repr_().render_html()

    def __str__(self):
        return self._table_repr_().render_text()

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
        elif isinstance(item, list):
            if len(item) == 0:
                return []
            elif isinstance(item[0], int):
                return NodeList(self._nodes[n] for n in item)
            else:
                raise ValueError("Unsupported indexing type: %s" % repr(type(item)))
        elif isinstance(item, slice):
            return NodeList(n for n in self._nodes[item] if n is not None)
        else:
            try:
                output = []
                for n in iter(item):
                    output.append(self[n])

                return NodeList(output)
            except TypeError:
                pass

            raise ValueError("Unsupported indexing type: %s" % repr(type(item)))

    def __delitem__(self, key):
        if isinstance(key, Node):
            self.remove(key)
        elif isinstance(key, int):
            n = self._nodes[key]
            self.remove(n)
        else:
            try:
                for n in iter(key):
                    del self[n]
            except TypeError:
                pass

            raise ValueError("Do not know how to remove: %s" % repr(key))

    def __contains__(self, item: Node):
        return item.collection is self


def codec_encode_span(l, v: "TextSpan"):
    if v is None:
        l.append(None)
        l.append(None)
    else:
        l.append(v.start_offset._id)
        l.append(v.stop_offset._id)


class Codec:
    encoders = {
        TEnum.I32: lambda l, v: l.append(None if v is None else int(v)),
        TEnum.I64: lambda l, v: l.append(None if v is None else int(v)),
        TEnum.F64: lambda l, v: l.append(None if v is None else float(v)),
        TEnum.BOOL: lambda l, v: l.append(None if v is None else bool(v)),
        TEnum.STRING: lambda l, v: l.append(None if v is None else str(v)),
        TEnum.BINARY: lambda l, v: l.append(None if v is None else bytes(v)),
        TEnum.NODEREF: lambda l, v: l.append(None if v is None else v._id),
        TEnum.NODEREF_MANY: lambda l, v: l.append(None if v is None or len(v) == 0 else [n._id for n in v]),
        TEnum.SPAN: codec_encode_span,
    }

    @staticmethod
    def encode(doc: "Document", fail_on_extra_fields=True):
        doc.compile(extra_fields_ok=fail_on_extra_fields)

        texts = {}
        types = {}
        types_num_nodes = {}
        schema = {}

        for txt in doc.texts.values():
            txt._gc()
            texts[txt.name] = txt._compile()

        # Encode types
        for k, v in doc.layers.items():
            propfields = {}
            typeschema = {}
            for field, fieldtype in v.schema.fields.items():
                typeschema[field] = fieldtype.encode()
                propvalues = []
                encoder = Codec.encoders[fieldtype.typename]
                for n in v:
                    encoder(propvalues, n.get(field, None))

                propfields[field] = propvalues

            types_num_nodes[k] = v.num
            
            schema[k] = typeschema
            types[k] = propfields

        return texts, types, types_num_nodes, schema


class Document:
    def __init__(self, **kwargs):
        """
        Construct new document

        :param kwargs: property key, values
        """
        self._layers = {}  # type: Dict[str, NodeLayerCollection]
        self._texts = {}  # type: Dict[str, Text]
        self.props = dict(kwargs)  # type: Dict[str, Any]

    @property
    def text(self):
        return self._texts

    @property
    def texts(self):
        return self._texts

    @property
    def layer(self):
        return self._layers

    @property
    def layers(self):
        return self._layers

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
                    if fv.typename == TEnum.NODEREF or fv.typename == TEnum.NODEREF_MANY:
                        if fv.options["layer"] == name:
                            referencing_layer_field.setdefault(k, []).append(fk)

        if not fieldcascade and len(referencing_layer_field) > 0:
            layer_field_names = ", ".join(
                map(lambda tup: "%s(%s)" % (tup[0], ", ".join(tup[1]))
                    , referencing_layer_field.items())
            )

            raise SchemaValidationError("Attempting to remove layer %s, but is referenced from layer(s)+field(s): %s"
                                        % (name, layer_field_names))

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
        """Prints the full schema of this document, containing layer fields and typing information"""
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

    def compile(self, extra_fields_ok=False, type_validation=True, **kwargs):
        """Compile the document, validates and assigns compacted ids to nodes (internal use)

        :param extra_fields_ok: ignores extra fields in node if set to True
        :param type_validation: do type validation, if set to False and type
                                is not correct will result in undefined behaviour, possibly corrupt storage.
        :raises SchemaValidationError
        """

        # Reset offset counters
        for txt in self.texts.values():
            txt.reset_counter()

        # Extract layer and text references
        referenced_layers = set()
        referenced_texts = set()
        for layer in self.layers.values():
            for field, fieldtype in layer.schema.fields.items():
                if fieldtype.typename == TEnum.NODEREF or fieldtype.typename == TEnum.NODEREF_MANY:
                    referenced_layers.add(fieldtype.options["layer"])
                elif fieldtype.typename == TEnum.SPAN:
                    referenced_texts.add(fieldtype.options["context"])

        # Verify refernced layers
        for layer in referenced_layers:
            if layer not in self.layers:
                raise SchemaError("Layer %s could not be found, "
                                  "it was referenced by another layer." % layer)

        # Verify referenced texts
        for text in referenced_texts:
            if text not in self.texts:
                raise SchemaError("Text with context '%s' could not be found, "
                                  "it was referenced by another layer." % text)

        # Assign node ids and validate nodes
        for k, v in self.layers.items():
            for idref, n in zip(range(len(v)), v):
                n._id = idref

            fieldtypes = v.schema.fields

            validate_fn = v.validate
            for n in v:
                if type_validation:
                    validate_fn(n)

                for field, fieldvalue in n.items():
                    if field in fieldtypes:
                        if isinstance(fieldvalue, TextSpan):
                            # Increase ref counts on span offsets (used to know which offsets to use)
                            fieldvalue.start_offset.incref()
                            fieldvalue.stop_offset.incref()
                    else:
                        if not extra_fields_ok:
                            key_set = set(n.keys())
                            key_set.difference_update(set(fieldtypes.keys()))

                            raise SchemaValidationError(
                                "Extra fields not declared in schema was found for layer %s, fields: %s" % (
                                    k, ", ".join(key_set)), key_set
                            )

            v.compact()
