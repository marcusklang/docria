#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Dict, List, Tuple, Callable, Any
from enum import Enum
import msgpack
import json
from io import BytesIO, IOBase
import logging

class PrintOptions:
    def __init__(self):
        """The maximum number of nodes to output, -1 for infinite"""
        self.max_nodes = 100
        self.trunc_col = 30

printoptions = PrintOptions()

class SchemaValidationError(Exception):
    """Schema validation failed"""
    def __init__(self, message, fields):
        super().__init__(message)
        self.fields = fields


class SchemaTypeError(Exception):
    """Type of node fields did not match schema types"""
    def __init__(self, message):
        super().__init__(message)


class DataError(Exception):
    """Serialization/Deserialization failure"""
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
    def __init__(self, **kwargs):
        self._id = -1  # type: int
        self.collection = None
        for k, v in kwargs.items():
            self[k] = v

    @property
    def fld(self):
        return DictAsMembers(self)

    def detach(self):
        """Remove itself from the document model"""
        self.collection.remove(self)

    def is_valid(self):
        """Verify if this node is still valid, i.e. is attached to an existing layer."""
        return self.collection is not None

    def __str__(self):
        return "%s#%d" % (self.collection.name, self._id)

    def __repr__(self):
        return "Node<%s[%d]%s>" % (self.collection.typedef.name, self._id,
                ", %s" % (", ".join([
                 "%s: %s" %
                 (k, repr(v) if not isinstance(v, Node) else
                     "Node(%s[%d])" % (v.collection.typedef.name, v._id))
                for k,v in self.items()
            ])) if len(self) > 0 else ""
        )


class Offset:
    """Text offset object"""
    def __init__(self, offset):
        self._id = -1
        self._refcnt = 0
        self.offset = offset

    def __index__(self):
        return self.offset

    def __eq__(self, off):
        return off is self or self.offset == off.offset

    def __hash__(self):
        return hash(self.offset)


class TextSpan:
    """Text span, consisting of a start and stop offset.

       Use str(span) to get a real string.
    """
    def __init__(self, text, startOffset, stopOffset):
        self.text = text
        self.startOffset = startOffset
        self.stopOffset = stopOffset

    def __hash__(self):
        return hash((self.startOffset, self.stopOffset))

    def __eq__(self, textrange):
        return (self.startOffset, self.stopOffset) == (textrange.startOffset, textrange.stopOffset)

    def __getitem__(self, indx: slice):
        raise NotImplementedError("TODO")

    def __repr__(self):
        return "Span(%s[%d:%d]=%s)" % (
            self.text.name,
            self.startOffset.offset,
            self.stopOffset.offset,
            repr(self.text.text[self.startOffset.offset:self.stopOffset.offset])
        )

    def __str__(self):
        return self.text.text[self.startOffset.offset:self.stopOffset.offset]


class Text:
    """Text object, consiting of text and an index of current offsets"""
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
        offset_objs = [] # type: List[Offset]
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

    def _gc(self):
        """Remove unused offsets. Assumes reference counter has been properly initialized."""
        self._offsets = {k: v for k,v in self._offsets.items() if v._refcnt > 0}

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
            if indx.step is not None and indx.step > 1:
                raise NotImplementedError("Only step == 1 are supported.")

            startOffset = self._offsets.setdefault(indx.start, Offset(indx.start))
            stopOffset = self._offsets.setdefault(indx.stop, Offset(indx.stop))
            return TextSpan(self, startOffset, stopOffset)
        elif isinstance(indx, tuple) and len(indx) == 2:
            startOffset = self._offsets.setdefault(int(indx[0]), Offset(int(indx[0])))
            stopOffset = self._offsets.setdefault(int(indx[1]), Offset(int(indx[1])))
            return TextSpan(self, startOffset, stopOffset)
        else:
            raise ValueError("Unsupported input indx: %s" % repr(indx))


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
    TEnum.SPAN: "span"
}

String2DataType = {v: k for k, v in DataType2String.items()}


class DataType:
    """Data type declaration"""
    def __init__(self, type: TEnum, **kwargs):
        self.type = type
        self.options = dict(kwargs)

    def encode(self):
        if len(self.options) > 0:
            return {
                "type": DataType2String[self.type],
                "args": self.options
            }
        else:
            return DataType2String[self.type]

    def __repr__(self):
        return "DataType(type=%s, options=%s)" % (DataType2String.get(self.type, str(self.type)), repr(self.options))

    def __hash__(self):
        return hash((self.type, tuple(sorted(self.options.items(),key=lambda x: x[0]))))

    def __eq__(self, dt):
        return self is dt or (isinstance(dt, DataType)
                              and (
                                  (self.type, tuple(sorted(self.options.items(), key=lambda x: x[0])))
                                  == (dt.type, tuple(sorted(dt.options.items(), key=lambda x: x[0])))
                                )
                              )


class DataTypes:
    """Common datatypes and factory methods for parametrical types"""
    float64 = DataType(TEnum.F64)
    int32 = DataType(TEnum.I32)
    int64 = DataType(TEnum.I64)
    string = DataType(TEnum.STRING)
    binary = DataType(TEnum.BINARY)
    bool = DataType(TEnum.BOOL)
    span = lambda context: DataType(TEnum.SPAN, context=context)
    noderef = lambda layer: DataType(TEnum.NODEREF, layer=layer)
    noderef_many = lambda layer: DataType(TEnum.NODEREF_MANY, layer=layer)

    @staticmethod
    def typeof(o, comparetype: "DataType"=None) -> DataType:
        if isinstance(o, str):
            return DataTypes.string
        elif isinstance(o, int):
            if comparetype is not None and comparetype.type == TEnum.I32:
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
            elif len(o) == 0 and comparetype is not None and comparetype.type == TEnum.NODEREF_MANY:
                return comparetype
            elif len(o) == 0:  # Is invalid in any case as the comparetype is unknown or not a list type.
                return DataType(TEnum.UNKNOWN)
            else:
                layer_name = o[0].collection.name
                if sum(1 for n in o if isinstance(n, Node) and n.collection.name == layer_name) == len(o):
                    return DataTypes.noderef_many(layer_name)
                else:
                    raise ValueError("Unsupported type: %s" % type(o))

        else:
            raise ValueError("Unsupported type: %s" % type(o))


class NodeLayer:
    """
    Node layer declaration

    Consists of name and field type declarations
    """

    def __init__(self, name):
        self.name = name
        self.fields = {}  # type: Dict[str, DataType]

    def add(self, name: str, fieldtype: "DataType"):
        if name in self.fields:
            raise ValueError("Field %s already exists on layer %s" % (name, self.name))

        self.fields[name] = fieldtype
        return self

    def set(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.fields:
                raise ValueError("Field %s already exists on layer %s" % (k, self.name))

            self.fields[k] = v

        return self


class ReprTable:
    def __init__(self, header, columns, id_field=False, hide_colnames=False, padding=2):
        self.padding = 2
        self.id_field = id_field
        self.columns = columns
        self.num_columns = len(columns) if isinstance(columns, list) else columns
        self.header = header
        self.data = [list() for _ in range(self.num_columns)]
        self.rows = []
        self.hide_colnames = hide_colnames

    def add(self, row):
        self.rows.append(row)

    def _text_range(self, indices):
        output = []
        for indx in indices:
            row = self.rows[indx]
            cols = [str(row[i]) if row[i] is not None else " NIL " for i in range(self.num_columns)]
            for i in range(len(cols)):
                if len(cols[i]) > printoptions.trunc_col+3:
                    cols[i] = cols[i][0:printoptions.trunc_col >> 1] + "..." + cols[i][-(printoptions.trunc_col >> 1):]

            output.append(cols)

        return output

    def text(self, num=-1):
        output = []

        incomplete = False

        if num == -1 or len(self.rows) < num:
            data = self._text_range(range(len(self.rows)))
        else:
            data = self._text_range(range(0, num))
            incomplete = True

        max_lengths = [max(map(len, map(lambda item: item[i], data)), default=0) for i in range(self.num_columns)]

        column_lens = list(map(len, self.columns)) if isinstance(self.columns, list) else [0] * self.num_columns
        max_lengths = [max(x, y) for x, y in zip(column_lens, max_lengths)]

        if self.id_field:
            header_str = ("%s%s%s" % (" " * self.padding, "{:<%d}" % (max_lengths[0]), " " * self.padding)) + \
             "".join([("%s%s%s" % (" " * self.padding, "{:<%d}" % (max_lengths[i]), " " * self.padding)) for i in range(1, len(max_lengths))])
        else:
            header_str = "".join([("%s%s%s" % (" " * self.padding, "{:<%d}" % (max_lengths[i]), " " * self.padding)) for i in range(len(max_lengths))])

        fmt_str = "".join([("%s%s%s" % (" " * self.padding, "{:<%d}" % (max_lengths[i]), " " * self.padding)) for i in range(len(max_lengths))])

        # Header
        if self.header is not None:
            output.append("")
            header_fmt = "--{: <%d}{:-<%d}" % (self.padding, sum(map(lambda x: x+self.padding*2, max_lengths)))
            output.append(header_fmt.format("", self.header + (" " * self.padding)))

        if not self.hide_colnames:
            output.append("")
            output.append(header_str.format(*self.columns))

        output.append("")

        for row in data:
            output.append(fmt_str.format(*row))

        output.append("")
        if incomplete:
            output.append("Only showing %d/%d" % (len(data), len(self.data)))

        return "\n".join(output)



class NodeLayerCollection:
    """
    Node collection, internally a skip-list which will compact when 25% of the list is empty.
    """
    def __init__(self, typedef: "NodeLayer"):
        self.nodetype = DataTypes.noderef(typedef.name)
        self.typedef = typedef
        self._nodes = []
        self.num = 0

    @property
    def name(self):
        return self.typedef.name

    def unsafe_initialize(self, nodes: List[Node]):
        """Directly replaces all nodes with the provided list, no checks for performance.

           Remarks: Only use this method if you know what you are doing!"""

        self.num = len(nodes)
        self._nodes = nodes
        for node_id, node in zip(range(len(self._nodes)), self._nodes):
            node._id = node_id
            node.collection = self

    def validate(self, node):
        return True

    def add(self, **kwargs) -> Node:
        node = Node(**kwargs)
        #self.validate(node)
        node.collection = self
        node._id = len(self._nodes)
        self._nodes.append(node)
        self.num += 1
        return node

    def compact(self):
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

    def remove(self, node):
        self._nodes[node._id] = None
        self.num -= 1

        if 0.75*len(self._nodes) > self.num:
            self.compact()
        
        node.collection = None

    def __iter__(self):
        for node in self._nodes:
            if node is None:
                continue

            yield node

    def __repr__(self):
        cols = [""] + list(sorted(self.typedef.fields.keys()))
        fields = [None] + list(sorted(self.typedef.fields.keys()))

        table = ReprTable(id_field=True, header="Layer: %s" % self.name, columns=cols)
        for n in self:
            fld_data = [n.get(fld, None) if fld is not None else "#%d" % n._id for fld in fields]
            for i in range(len(fld_data)):
                if isinstance(fld_data[i], list):
                    if len(fld_data[i]) > 0:
                        all_nodes = True
                        nodetypes = set()
                        for elem in fld_data[i]:
                            if isinstance(elem, Node):
                                nodetypes.add(elem.collection.name)
                            else:
                                all_nodes = False
                                break

                        if all_nodes:
                            fld_data[i] = "[%d nodes, layer: %s]" % (len(fld_data[i]), ", ".join(nodetypes))
                        else:
                            fld_data[i] = "[List with %d elements]" % len(fld_data[i])

            table.add(fld_data)

        return table.text(num=printoptions.max_nodes)

    def __len__(self):
        return self.num

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._nodes[item]
        elif isinstance(item, list):
            if len(item) == 0:
                return []
            elif isinstance(item[0], int):
                return [self._nodes[n] for n in item]
            else:
                raise ValueError("Unsupported indexing type: %s" % repr(type(item)))
        elif isinstance(item, slice):
            return [n for n in self._nodes[item] if n is not None]
        else:
            raise ValueError("Unsupported indexing type: %s" % repr(type(item)))

    def __delitem__(self, key):
        n = self._nodes[key]
        self.remove(n)

    def __contains__(self, item: Node):
        return item.collection is self


def codec_encode_span(l, v: "TextSpan"):
    if v is None:
        l.append(None)
        l.append(None)
    else:
        l.append(v.startOffset._id)
        l.append(v.stopOffset._id)


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
        doc.compile(fail_on_extra_fields=fail_on_extra_fields)

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
            for field, fieldtype in v.typedef.fields.items():
                typeschema[field] = fieldtype.encode()
                propvalues = []
                encoder = Codec.encoders[fieldtype.type]
                for n in v:
                    encoder(propvalues, n.get(field, None))

                propfields[field] = propvalues

            types_num_nodes[k] = v.num
            
            schema[k] = typeschema
            types[k] = propfields

        return texts, types, types_num_nodes, schema


class JsonCodec:
    @staticmethod
    def encode(doc: "Document"):
        texts, types, types_num_nodes, schema = Codec.encode(doc)

        return json.dumps({
            "DM10": {
                "props": doc.props,
                "texts": texts,
                "num_nodes": types_num_nodes,
                "types": types,
                "schema": schema
            }
        })

    @staticmethod
    def decode(docstr):
        docobj = json.loads(docstr)
        if not isinstance(docobj, dict):
            raise DataError("JSON object is not a dictionary => cannot be a document.")

        if "DM10" not in docobj:
            raise DataError("Unsupported document, no supported headers found, fields: %s" % ", ".join(list(docobj.keys())))

        docobj = docobj["DM10"]

        doc = Document()
        doc.props = docobj["props"]

        schema = {}  # type: Dict[str, List[Tuple[str, DataType]]]

        for typename, fieldtypes in docobj["schema"].items():
            nl = NodeLayer(typename)

            fields = []

            for fieldname, typedef in fieldtypes.items():
                if isinstance(typedef, dict): # Advanced type
                    ttype = String2DataType[typedef["type"]]
                    args = typedef["args"]
                    fields.append((fieldname, DataType(ttype, **args)))
                elif isinstance(typedef, str): # Simple type
                    ttype = String2DataType[typedef]
                    fields.append((fieldname, DataType(ttype)))
                else:
                    raise DataError("Could not decode layer %s field types, "\
                                    "failed on field %s. Got data: %s" % (typename, fieldname, repr(typedef)))

                nl.add(fieldname, fields[-1][1])

            schema[typename] = fields

        texts = docobj["texts"]
        text2offsets = {}
        for textname, text in texts.items():
            offsets = []
            pos = 0
            for subseq in text:
                offsets.append(pos)
                pos += len(subseq)

            offsets.append(pos)

            fulltext = "".join(text)

            textobj = doc.add_text(textname, fulltext)
            text2offsets[textname] = textobj.initialize_offsets(offsets)

        all_nodes = {}
        types_num_nodes = docobj["num_nodes"]

        for typename in schema.keys():
            num_nodes = types_num_nodes[typename]
            nodes = [Node() for _ in range(num_nodes)]
            all_nodes[typename] = nodes

            for col, typedef in schema[typename]:
                def simple_field(data):
                    nonlocal nodes

                    for n, v in zip(nodes, data):
                        if v is not None:
                            n[col] = v

                def span_field(text, offsets):
                    nonlocal nodes

                    def decoder(data):
                        for n, v in zip(nodes, range(int(len(data)/2))):
                            if data[v*2] is not None:
                                n[col] = TextSpan(text, offsets[data[v * 2]], offsets[data[v * 2 + 1]])

                    return decoder

                decoder = simple_field
                if typedef.type == TEnum.SPAN:
                    decoder = span_field(doc.texts[typedef.options["context"]], text2offsets[typedef.options["context"]])

                coldata = docobj["types"][typename][col]
                decoder(coldata)

        # Insert layers
        for typename in schema.keys():

            # Create Node Type
            nt = NodeLayer(typename)
            for col, typedef in schema[typename]:
                if typedef.type == TEnum.NODEREF:

                    # Replace int placeholds with an actual node reference.
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            n[col] = target_nodes[n[col]]
                elif typedef.type == TEnum.NODEREF_MANY:
                    # Replace int placeholds with an actual node reference.
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            n[col] = [target_nodes[n_item] for n_item in n[col]]

                nt.add(col, typedef)

            layer = doc.add_layer(nt)
            layer.unsafe_initialize(all_nodes[typename])

        return doc


class MsgpackCodec:
    @staticmethod
    def debug(data):
        if isinstance(data, bytes):
            data = BytesIO(data)
        elif isinstance(data, IOBase):
            pass

        unpacker = msgpack.Unpacker(data, raw=False)
        header = unpacker.read_bytes(4)

        print("-- Content --")
        print("Magic: %s" % repr(header))

        print("Document properties:")
        docprop_sz = next(unpacker)
        print(next(unpacker))

        print("Types:")
        types = next(unpacker)
        print(types)

        schema = {}

        print("Schema: ")
        for typename in types:
            print(" * %s" % typename)
            num_fields = next(unpacker)

            fields = []

            for i in range(num_fields):
                fieldname = next(unpacker)
                has_args = next(unpacker)
                fieldtype = next(unpacker)
                if has_args:
                    fieldargs = next(unpacker)
                    fields.append( (fieldname, {"type": fieldtype, "args": fieldargs}) )
                else:
                    fields.append( (fieldname, fieldtype) )

                print("  - %s = %s" % fields[-1])


            schema[typename] = fields

        print("Texts: ")
        texts_len = next(unpacker)
        print(" * Length: %d bytes" % texts_len)
        texts = next(unpacker)

        for k, v in texts.items():
            print("%s = %s" % (k, repr(v)))

        print("Types data:")
        for typename in types:
            print("[%s]" % typename)
            datalength = next(unpacker)
            num_nodes = next(unpacker)

            print(" * Segment length: %d " % datalength)
            print(" * Num nodes: %d" % num_nodes)

            for col, typedef in schema[typename]:
                print("  - %s" % col)
                print("  - Special encoding: %s" % repr(next(unpacker)))

                print(" ==> %s" % next(unpacker))


    @staticmethod
    def encode(doc, fail_on_extra_fields=True):
        """
        Encode document using MessagePack encoder
        :param doc: the document to encode
        :param fail_on_extra_fields: verify and fail if fields not in schema are found in nodes.
        :return: bytes of the document
        """
        texts, types, types_num_nodes, schema = Codec.encode(doc, fail_on_extra_fields=fail_on_extra_fields)
        output = BytesIO()
        typelist = list(types.keys())

        output.write(b"DM_1")

        # 1. Write Document properties
        out_props = BytesIO()
        msgpack.pack(doc.props, out_props)

        msgpack.pack(out_props.tell(), output)
        output.write(out_props.getbuffer()[0:out_props.tell()])

        # 2. Write Inventory of types
        msgpack.pack(typelist, output, use_bin_type=True)
        types2columns = {}

        # 3. Write Schema
        for typename in typelist:
            type_def = schema[typename]
            msgpack.pack(len(type_def), output, use_bin_type=True)

            layer_cols = []
            for k, v in type_def.items():
                layer_cols.append(k)

                msgpack.pack(k, output, use_bin_type=True)
                if isinstance(v, str):
                    msgpack.pack(False, output)
                    msgpack.pack(v, output, use_bin_type=True)
                elif isinstance(v, dict):
                    msgpack.pack(True, output)
                    msgpack.pack(v["type"], output, use_bin_type=True)
                    msgpack.pack(v["args"], output, use_bin_type=True)
                else:
                    raise NotImplementedError()

            types2columns[typename] = layer_cols

        out_texts = BytesIO()

        # 4. Write Texts
        msgpack.pack(texts, out_texts, use_bin_type=True)

        msgpack.pack(out_texts.tell(), output)
        output.write(out_texts.getbuffer()[0:out_texts.tell()])

        # 5. Write Type data
        out_types = BytesIO()
        for typename in typelist:
            out_type = BytesIO()
            msgpack.pack(types_num_nodes[typename], out_type)
            for col in types2columns[typename]:
                msgpack.pack(False, out_type)  # Future support for specialized encoding
                msgpack.pack(types[typename][col], out_type, use_bin_type=True)

            msgpack.pack(out_type.tell(), out_types)
            out_types.write(out_type.getbuffer()[0:out_type.tell()])

        output.write(out_types.getbuffer()[0:out_types.tell()])
        return output.getvalue()

    @staticmethod
    def decode(data):
        if isinstance(data, bytes):
            data = BytesIO(data)
        elif isinstance(data, IOBase):
            pass

        unpacker = msgpack.Unpacker(data, raw=False)
        header = unpacker.read_bytes(4)

        if header != b"DM_1":
            raise ValueError("Magic bytes is not DM_1")

        prop_sz = next(unpacker)

        doc = Document()
        doc.props = next(unpacker)

        types = next(unpacker)
        schema = {}  # type: Dict[str, List[Tuple[str, DataType]]]

        for typename in types:
            num_fields = next(unpacker)

            nl = NodeLayer(typename)

            fields = []

            for i in range(num_fields):
                fieldname = next(unpacker)
                has_args = next(unpacker)
                fieldtype = next(unpacker)
                ttype = String2DataType[fieldtype]

                if has_args:
                    fieldargs = next(unpacker)
                    fields.append((fieldname, DataType(ttype, **fieldargs)))
                else:
                    fields.append((fieldname, DataType(ttype)))

                nl.add(fieldname, fields[-1][1])

            schema[typename] = fields

        texts_len = next(unpacker)
        texts = next(unpacker)

        text2offsets = {}
        for textname, text in texts.items():
            offsets = []
            pos = 0
            for subseq in text:
                offsets.append(pos)
                pos += len(subseq)

            offsets.append(pos)

            fulltext = "".join(text)

            textobj = doc.add_text(textname, fulltext)
            text2offsets[textname] = textobj.initialize_offsets(offsets)

        all_nodes = {}

        for typename in types:
            datalength = next(unpacker)
            num_nodes = next(unpacker)
            nodes = [Node() for _ in range(num_nodes)]
            all_nodes[typename] = nodes

            for col, typedef in schema[typename]:
                def simple_field(data):
                    nonlocal nodes

                    for n, v in zip(nodes, data):
                        if v is not None:
                            n[col] = v

                def span_field(text, offsets):
                    nonlocal nodes

                    def decoder(data):
                        if text is None and len(data) > 0:
                            logging.warning("Node field is referring to non existant context: %s, "
                                            "cannot decode this field: %s in %s. "
                                            "Field ignored." % (typedef.options["context"], col, typename))
                        else:
                            for n, v in zip(nodes, range(int(len(data)/2))):
                                if data[v*2] is not None:
                                    n[col] = TextSpan(text, offsets[data[v * 2]], offsets[data[v * 2 + 1]])

                    return decoder

                decoder = simple_field
                if typedef.type == TEnum.SPAN:
                    decoder = span_field(
                        doc.texts.get(typedef.options["context"], None),
                        text2offsets.get(typedef.options["context"], None))

                special_encoding = next(unpacker)
                if special_encoding:
                    raise NotImplementedError("special_encoding")

                coldata = next(unpacker)
                decoder(coldata)

        # Insert layers
        for typename in types:

            # Create Node Type
            nt = NodeLayer(typename)
            for col, typedef in schema[typename]:
                if typedef.type == TEnum.NODEREF:

                    # Replace int placeholds with an actual node reference.
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            n[col] = target_nodes[n[col]]
                elif typedef.type == TEnum.NODEREF_MANY:
                    # Replace int placeholds with an actual node reference.
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            n[col] = [target_nodes[n_item] for n_item in n[col]]

                nt.add(col, typedef)

            layer = doc.add_layer(nt)
            layer.unsafe_initialize(all_nodes[typename])

        return doc


class Document:
    def __init__(self, **kwargs):
        self.layers = {} # type: Dict[str, NodeLayerCollection]
        self.texts = {} # type: Dict[str, Text]
        self.props = dict(kwargs)  # type: Dict[str, Any]

    def add_text(self, name, text):
        txtobj = Text(name, text)
        self.texts[name] = txtobj
        return txtobj

    def add_layer(self, __name: str, **kwargs):
        if isinstance(__name, NodeLayer):
            typedef = __name
        else:
            typedef = NodeLayer(__name)
            typedef.set(**kwargs)

        typecol = NodeLayerCollection(typedef)
        self.layers[typedef.name] = typecol
        return typecol

    def __repr__(self):
        output = ["== Document =="]
        tblTexts = ReprTable(header="Texts", columns=["Name", "Excerpt"], hide_colnames=True)

        for k, v in self.texts.items():
            tblTexts.add([k, v.text[0:100] + "..." if len(v.text) > 100 else v.text])

        output.append(tblTexts.text(-1))

        tblLayers = ReprTable(header="Layers", columns=2, hide_colnames=True)
        for k, v in self.layers.items():
            tblLayers.add([k, "N={:}".format(len(v))])

        output.append(tblLayers.text(-1))

        return "".join(output)

    def printschema(self):
        for k, v in self.layers.items():
            print("[%s]" % k)
            max_length = max(map(len, v.typedef.fields.keys()), default=0)

            for field, fieldtype in v.typedef.fields.items():
                print((" * {:<%d} : {:}{:}" % max_length).format(
                    field,
                    DataType2String[fieldtype.type],
                    "" if len(fieldtype.options) == 0
                    else "[%s]" % ", ".join(map(lambda tup: "%s=%s" % tup, fieldtype.options.items()))
                ))
            print()

    def __getstate__(self):
        return {
            "msgpacked": MsgpackCodec.encode(self)
        }

    def __setstate__(self, state):
        doc = MsgpackCodec.decode(state["msgpacked"])
        self.layers = doc.layers
        self.texts = doc.texts
        self.props = doc.props

    def compile(self, fail_on_extra_fields=True):
        for txt in self.texts.values():
            txt.reset_counter()

        # TODO: Validate schema, make sure that layers which are referenced exists or create empty placeholders.

        # Assign node ids and validate nodes
        for k, v in self.layers.items():
            for idref, n in zip(range(len(v)), v):
                n._id = idref

            fieldtypes = v.typedef.fields

            for n in v:
                for field, fieldvalue in n.items():
                    if field in fieldtypes:
                        valuetype = DataTypes.typeof(fieldvalue, fieldtypes[field])
                        if valuetype != fieldtypes[field]:
                            raise SchemaTypeError("Invalid node, typeof(%s) = %s does not match %s. Ref: %s" % (repr(fieldvalue), repr(valuetype), repr(fieldtypes[field]), repr(n)))

                        if isinstance(fieldvalue, TextSpan):
                            fieldvalue.startOffset._refcnt += 1
                            fieldvalue.stopOffset._refcnt += 1
                    else:
                        if fail_on_extra_fields:
                            key_set = set(n.keys())
                            key_set.difference_update(set(fieldtypes.keys()))

                            raise SchemaValidationError(
                                "Extra fields not declared in schema was found for layer %s, fields: %s" % (
                                    k, ", ".join(key_set)), key_set
                            )

            v.compact()

