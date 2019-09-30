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
"""Codecs, encoding/decoding documents to/from binary or text representations"""
from docria.model import TextSpan, Document, DataTypeEnum, NodeLayerSchema, String2DataType, DataType, Node, ExtData, NodeSpan
import msgpack
import json
import logging
from io import BytesIO, IOBase
from typing import List, Dict, Tuple
from base64 import standard_b64decode, standard_b64encode
import re


class DataError(Exception):
    """Serialization/Deserialization failure"""
    def __init__(self, message):
        super().__init__(message)


def _codec_encode_span(offset_mapping: Dict[int, int]):
    def encoder(l, v: "TextSpan"):
        if v is None:
            l.append(None)
            l.append(None)
        else:
            l.append(offset_mapping[v.start])
            l.append(offset_mapping[v.stop])
    return encoder


class Codec:
    """Utility methods for all codecs"""
    encoders = {
        DataTypeEnum.I32: lambda l, v: l.append(None if v is None else int(v)),
        DataTypeEnum.I64: lambda l, v: l.append(None if v is None else int(v)),
        DataTypeEnum.F64: lambda l, v: l.append(None if v is None else float(v)),
        DataTypeEnum.BOOL: lambda l, v: l.append(None if v is None else bool(v)),
        DataTypeEnum.STRING: lambda l, v: l.append(None if v is None else str(v)),
        DataTypeEnum.BINARY: lambda l, v: l.append(None if v is None else bytes(v)),
        DataTypeEnum.NODEREF: lambda l, v: l.append(None if v is None else v.i),
        DataTypeEnum.NODEREF_MANY: lambda l, v: l.append(None if v is None or len(v) == 0 else [n.i for n in v]),
        DataTypeEnum.NODEREF_SPAN: lambda l, v: l.append(None if v is None else [v.left.i, v.right.i - v.left.i]),
        DataTypeEnum.SPAN: _codec_encode_span
    }

    @staticmethod
    def encode(doc: "Document", doc_encoder, **kwargs):
        offset_mapping = doc.compile(**kwargs)

        texts = {}
        types = {}
        types_num_nodes = {}
        schema = {}

        for txt in doc.texts.values():
            texts[txt.name] = txt.compile(offset_mapping[txt.name][1])

        node_getter = Node.get

        # Encode types
        for k, v in doc.layers.items():
            propfields = {}
            typeschema = {}
            for field, fieldtype in v.schema.fields.items():
                typeschema[field] = fieldtype.encode()

                propvalues = []
                encoder = Codec.encoders[fieldtype.typename]
                if fieldtype.typename == DataTypeEnum.EXT:
                    for n in v:
                        extv = node_getter(n, field, None)
                        if extv is not None:
                            if isinstance(extv, bytes):
                                propvalues.append(extv)
                            elif isinstance(extv, ExtData):
                                propvalues.append(extv.encode())
                            else:
                                raise ValueError("Incorrect value.")
                        else:
                            propvalues.append(None)

                        propvalues.append(None if extv is None else extv.encode())
                elif fieldtype.typename == DataTypeEnum.SPAN:
                    encoder = encoder(offset_mapping[fieldtype.options["context"]][0])
                    for n in v:
                        encoder(propvalues, node_getter(n, field, None))
                else:
                    for n in v:
                        encoder(propvalues, node_getter(n, field, None))

                propfields[field] = propvalues

            types_num_nodes[k] = v.num

            schema[k] = typeschema
            types[k] = propfields

        return texts, types, types_num_nodes, schema

    @staticmethod
    def commit_layers(doc: "Document",
                      types: List[str],
                      schema: Dict[str, List[Tuple[str, any]]],
                      all_nodes: Dict[str, List[Node]]):
        """
        Do post-processing after deserialization phase, for instance replace node ids with node references.

        :param doc: the document
        :param types: layer names
        :param schema: schema definition
        :param all_nodes: dictionary of all nodes
        """
        # TODO: Replace types with schema being an OrderedDict

        # Insert layers
        for typename in types:

            # Create Node Type
            nt = NodeLayerSchema(typename)
            for col, typedef in schema[typename]:
                nt.add(col, typedef)

            layer = doc.add_layer(nt)
            for n in all_nodes[typename]:
                n.collection = layer

            layer.unsafe_initialize(all_nodes[typename])

        # Post-process layers
        for typename in types:
            for col, typedef in schema[typename]:
                if typedef.typename == DataTypeEnum.NODEREF:

                    # Replace int placeholds with an actual node reference.
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            n[col] = target_nodes[n[col]]
                elif typedef.typename == DataTypeEnum.NODEREF_MANY:
                    # Replace int placeholds with an actual node reference.
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            n[col] = [target_nodes[n_item] for n_item in n[col]]
                elif typedef.typename == DataTypeEnum.NODEREF_SPAN:
                    # Replace [int, int] with NodeSpan(left, right) which are real node references
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            lst = n[col]
                            left_i, right_i = lst[0], lst[0]+lst[1]  # Delta encoded length
                            n[col] = NodeSpan(target_nodes[left_i], target_nodes[right_i])


class JsonCodec:
    """JSON codec"""
    @staticmethod
    def encode(doc: "Document"):
        return json.dumps(JsonCodec.encode_object(doc))

    @staticmethod
    def encode_object(doc: "Document"):
        texts, types, types_num_nodes, schema = Codec.encode(doc, doc_encoder=JsonCodec.encode_object)
        return {
            "DM10": {
                "props": doc.props,
                "texts": texts,
                "num_nodes": types_num_nodes,
                "types": types,
                "schema": schema
            }
        }

    @staticmethod
    def decode(docstr):
        docobj = json.loads(docstr)
        if not isinstance(docobj, dict):
            raise DataError("JSON object is not a dictionary => cannot be a document.")

        if "DM10" not in docobj:
            raise DataError("Unsupported document, no supported headers found, fields: %s" % ", ".join(list(docobj.keys())))

        return JsonCodec.decode_object(docobj)

    @staticmethod
    def decode_object(docobj):
        docobj = docobj["DM10"]

        doc = Document()
        doc.props = docobj["props"]

        schema = {}  # type: Dict[str, List[Tuple[str, DataType]]]

        for typename, fieldtypes in docobj["schema"].items():
            nl = NodeLayerSchema(typename)

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
                    raise DataError("Could not decode layer %s field types, " \
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

            doc.add_text(textname, fulltext)
            text2offsets[textname] = dict(zip(range(len(offsets)), offsets))

        all_nodes = {}
        types_num_nodes = docobj["num_nodes"]

        for typename in schema.keys():
            num_nodes = types_num_nodes[typename]
            nodes = [Node().with_id(i) for i in range(num_nodes)]
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

                def ext_field(data):
                    nonlocal nodes

                    typename = typedef.options["type"]
                    if typename == "doc":
                        extdata = MsgpackDocumentExt
                    else:
                        extdata = lambda v: ExtData(typename, v)

                    for n, v in zip(nodes, data):
                        if v is not None:
                            n[col] = typename(standard_b64decode(data))

                decoder = simple_field
                if typedef.typename == DataTypeEnum.SPAN:
                    decoder = span_field(doc.texts[typedef.options["context"]], text2offsets[typedef.options["context"]])

                if typedef.typename == DataTypeEnum.EXT:
                    decoder = ext_field

                coldata = docobj["types"][typename][col]
                decoder(coldata)

        # TODO: Replace with Codec.commit_layers
        # Insert layers
        for typename in schema.keys():

            # Create Node Type
            nt = NodeLayerSchema(typename)
            for col, typedef in schema[typename]:
                nt.add(col, typedef)

            layer = doc.add_layer(nt)
            for n in all_nodes[typename]:
                n.collection = layer

            layer.unsafe_initialize(all_nodes[typename])

        # Post-process layers
        for typename in schema.keys():
            for col, typedef in schema[typename]:
                if typedef.typename == DataTypeEnum.NODEREF:

                    # Replace int placeholds with an actual node reference.
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            n[col] = target_nodes[n[col]]
                elif typedef.typename == DataTypeEnum.NODEREF_MANY:
                    # Replace int placeholds with an actual node reference.
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            n[col] = [target_nodes[n_item] for n_item in n[col]]
                elif typedef.typename == DataTypeEnum.NODEREF_SPAN:
                    # Replace [int, int] with NodeSpan(left, right) which are real node references
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            lst = n[col]
                            left_i, right_i = lst[0], lst[0]+lst[1]  # Delta encoded length
                            n[col] = NodeSpan(target_nodes[left_i], target_nodes[right_i])

        return doc


class MsgpackDocument:
    """MessagePack Document, allows partial decoding"""
    def __init__(self, rawdata, ref=None):
        self.ref = ref
        if isinstance(rawdata, bytes):
            rawdata = BytesIO(rawdata)
        elif isinstance(rawdata, IOBase):
            pass

        self.rawdata = rawdata

        if rawdata.read(4) != b"DM_1":
            raise ValueError("Magic bytes is not DM_1")

        self._read_state = 0
        self._prop = None
        self._texts = None
        self._schema = None
        self._layers = None

    def _parse_state(self, state):
        if self._read_state < 1 and state > 0:
            self.rawdata.seek(4)

            unpacker = msgpack.Unpacker(self.rawdata, raw=False)
            prop_sz = next(unpacker)
            prop_start = unpacker.tell() + 4

            self._prop = (prop_start, prop_sz)

        if self._read_state < 2 and state > 1:
            start_pos = self._prop[0] + self._prop[1]
            self.rawdata.seek(start_pos)
            unpacker = msgpack.Unpacker(self.rawdata, raw=False)

            types, schema = MsgpackCodec.decode_schema(unpacker)
            self._schema = types, schema

            texts_len = next(unpacker)
            texts_start = unpacker.tell()+start_pos
            self._texts = (texts_start, texts_len)

        if self._read_state < 3 and state > 2:
            start_pos = self._texts[0] + self._texts[1]
            self.rawdata.seek(start_pos)
            unpacker = msgpack.Unpacker(self.rawdata, raw=False)

            layer_mapping = {}
            for typename in self._schema[0]:
                layer_len = next(unpacker)
                layer_start = unpacker.tell() + start_pos
                layer_mapping[typename] = (layer_start, layer_len)
                self.rawdata.seek(layer_start+layer_len)
                unpacker = msgpack.Unpacker(self.rawdata, raw=False)
                start_pos = layer_start + layer_len

            self._layers = layer_mapping

    def binary(self)->bytes:
        """Get this document as binary value"""
        return self.rawdata.getvalue()

    def properties(self, *props):
        """Get document properties"""
        self._parse_state(1)
        self.rawdata.seek(self._prop[0])
        unpacker = msgpack.Unpacker(self.rawdata, raw=False)
        return MsgpackCodec.decode_property(unpacker, *props)

    def schema(self):
        """Get document schema"""
        self._parse_state(2)
        return self._schema[0], self._schema[1]

    def texts(self, *texts):
        """Get document text"""
        self._parse_state(2)
        self.rawdata.seek(self._texts[0])
        unpacker = msgpack.Unpacker(self.rawdata, raw=False)
        return MsgpackCodec.decode_texts(unpacker, *texts)

    def document(self, *layers, **kwargs):
        """Get fully decoded document"""
        self._parse_state(3)
        doc = Document()

        # -- Parse properties
        doc.props = self.properties()

        # -- Parse schema
        types, schema = self.schema() #MsgpackCodec.decode_schema(unpacker)

        # -- Parse texts
        texts = self.texts()# MsgpackCodec.decode_texts(unpacker)

        text2offsets = MsgpackCodec.compute_text_offsets(doc, texts)

        # -- Parse layers
        layer_set = types if len(layers) == 0 else list(layers)
        all_nodes = {}
        for typename in layer_set:
            self.rawdata.seek(self._layers[typename][0])
            unpacker = msgpack.Unpacker(self.rawdata, raw=False)
            # datalength = next(unpacker)
            #unpacker.skip()

            layerschema = schema[typename]
            all_nodes[typename] = MsgpackCodec.decode_layer(unpacker, doc, typename, text2offsets, layerschema, **kwargs)

        Codec.commit_layers(doc, types, schema, all_nodes)
        return doc


class MsgpackDocumentExt(ExtData):
    """Embeddable document as a extended type"""
    def __init__(self, doc):
        super().__init__("doc", doc)

    def encode(self):
        if isinstance(self.data, bytes):
            return self.data
        else:
            return MsgpackCodec.encode(self.data)

    def decode(self):
        if isinstance(self.data, Document):
            return self.data
        else:
            self.data = MsgpackCodec.decode(self.data)
            return self.data


class MsgpackCodec:
    """MessagePack document codec"""
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
    def encode(doc, **kwargs):
        """
        Encode document using MessagePack encoder

        :param doc: the document to encode
        :param kwargs: passed along to Codec.encode and Document.compile
        :raises SchemaValidationError
        :return: bytes of the document
        """
        texts, types, types_num_nodes, schema = Codec.encode(doc, doc_encoder=MsgpackCodec.encode, **kwargs)
        output = BytesIO()
        typelist = list(types.keys())

        output.write(b"DM_1")

        # 1. Write Document properties
        out_props = BytesIO()

        # TODO: Implement extension handling!
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
                # TODO: Implement extension handling!

            msgpack.pack(out_type.tell(), out_types)
            out_types.write(out_type.getbuffer()[0:out_type.tell()])

        output.write(out_types.getbuffer()[0:out_types.tell()])
        return output.getvalue()

    @staticmethod
    def decode_property(unpacker: msgpack.Unpacker, *props, **kwargs):
        if len(props) == 0:
            output = next(unpacker)  # type: dict
        else:
            prop_keys = set(props)
            output = dict()

            num_entries = unpacker.read_map_header()
            for i in range(num_entries):
                k = next(unpacker)
                if k in prop_keys:
                    v = next(unpacker)
                    output[k] = v
                else:
                    unpacker.skip()

        # TODO: Implement extension handling!
        return output

    @staticmethod
    def decode_schema(unpacker: msgpack.Unpacker):
        types = next(unpacker)
        schema = {}  # type: Dict[str, List[Tuple[str, DataType]]]

        for typename in types:
            num_fields = next(unpacker)

            nl = NodeLayerSchema(typename)

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

        return types, schema

    @staticmethod
    def decode_texts(unpacker, *texts):
        if len(texts) == 0:
            return next(unpacker)
        else:
            texts = {}

            prop_keys = set(texts)
            num_entries = unpacker.read_map_header()
            for i in range(num_entries):
                k = next(unpacker)
                if k in prop_keys:
                    v = next(unpacker)
                    texts[k] = v
                else:
                    unpacker.skip()

            return texts

    @staticmethod
    def compute_text_offsets(doc, texts):
        """Computes all offsets and inserts text into document"""

        text2offsets = {}
        for textname, text in texts.items():
            offsets = []
            pos = 0
            for subseq in text:
                offsets.append(pos)
                pos += len(subseq)

            offsets.append(pos)

            fulltext = "".join(text)

            doc.add_text(textname, fulltext)
            text2offsets[textname] = dict(zip(range(len(offsets)), offsets))

        return text2offsets

    @staticmethod
    def decode_layer(unpacker, doc, typename, text2offsets, layerschema, *fields, **kwargs):
        num_nodes = next(unpacker)
        nodes = [Node().with_id(i) for i in range(num_nodes)]

        fieldindx = None
        if len(fields) > 0:
            fieldindx = set(fields)

        for col, typedef in layerschema:
            if fieldindx is None or col in fieldindx:
                def simple_field(data):
                    nonlocal nodes

                    for n, v in zip(nodes, data):
                        if v is not None:
                            n[col] = v

                def doc_field(data):
                    nonlocal nodes

                    for n, v in zip(nodes, data):
                        if v is not None:
                            n[col] = MsgpackCodec.decode(v)

                def ext_field(data):
                    types = typedef.options["type"]
                    if types == "doc":
                        extdata = MsgpackDocumentExt
                    else:
                        extdata = lambda v: ExtData(types, v)

                    for n, v in zip(nodes, data):
                        if v is not None:
                            n[col] = extdata(v.data)

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
                if typedef.typename == DataTypeEnum.SPAN:
                    decoder = span_field(
                        doc.texts.get(typedef.options["context"], None),
                        text2offsets.get(typedef.options["context"], None))
                elif typedef.typename == DataTypeEnum.EXT:
                    if typedef.options["type"] == "doc":
                        decoder = doc_field
                    else:
                        decoder = ext_field

                special_encoding = next(unpacker)
                if special_encoding:
                    raise NotImplementedError("special_encoding")

                coldata = next(unpacker)
                decoder(coldata)
            else:
                special_encoding = next(unpacker)
                if special_encoding:
                    raise NotImplementedError("special_encoding")

                unpacker.skip()

        return nodes

    @staticmethod
    def decode(data, **kwargs):
        """
        Decode message pack encoded document

        :param data: bytes or file-like object
        :return: Document instance
        """
        if isinstance(data, bytes):
            data = BytesIO(data)
        elif isinstance(data, IOBase):
            pass

        unpacker = msgpack.Unpacker(data, raw=False)
        header = unpacker.read_bytes(4)

        if header != b"DM_1":
            raise ValueError("Magic bytes is not DM_1")

        doc = Document()

        # prop_sz = next(unpacker)
        unpacker.skip()

        # -- Parse properties
        doc.props = MsgpackCodec.decode_property(unpacker, **kwargs)

        # -- Parse schema
        types, schema = MsgpackCodec.decode_schema(unpacker)

        # -- Parse texts

        # texts_len = next(unpacker)
        unpacker.skip()

        texts = MsgpackCodec.decode_texts(unpacker)

        text2offsets = MsgpackCodec.compute_text_offsets(doc, texts)

        # -- Parse layers
        all_nodes = {}
        for typename in types:
            # datalength = next(unpacker)
            unpacker.skip()

            layerschema = schema[typename]
            all_nodes[typename] = MsgpackCodec.decode_layer(unpacker, doc, typename,
                                                            text2offsets, layerschema, **kwargs)

        Codec.commit_layers(doc, types, schema, all_nodes)
        return doc


class XmlCodec:
    """XML Codec, only encoding support"""
    _string_pattern = re.compile(r"[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD\u10000-\u10FFFF]", re.UNICODE)

    @staticmethod
    def _string_encoder(s, repl=" "):
        return XmlCodec._string_pattern.sub(repl, s)

    """
    Docria XML codec
    """
    encoders = {
        DataTypeEnum.I32: lambda v: str(int(v)),
        DataTypeEnum.I64: lambda v: str(int(v)),
        DataTypeEnum.F64: lambda v: str(float(v)),
        DataTypeEnum.BOOL: lambda v: str(bool(v)),
        DataTypeEnum.STRING: lambda v: XmlCodec._string_encoder(v),
        DataTypeEnum.BINARY: lambda v: standard_b64encode(bytes(v)),
        DataTypeEnum.NODEREF: lambda v: str(v._id),
        DataTypeEnum.NODEREF_MANY: lambda v: [str(n._id) for n in v],
        DataTypeEnum.NODEREF_SPAN: lambda v: v
    }

    @staticmethod
    def _ext_encoder(value):
        extv = value
        if extv is not None:
            if isinstance(extv, bytes):
                return extv
            elif isinstance(extv, ExtData):
                return extv.encode()
            else:
                raise ValueError("Incorrect value.")
        else:
            return None

    @staticmethod
    def _span_encoder(offset_mapping):
        def encoder(span):
            return offset_mapping, span

        return encoder

    @staticmethod
    def encode_intermediate(doc, **kwargs):
        """
        Conversion of docria document into an intermediate form: texts, schema and layer data.

        :param doc: docria document
        :param kwargs: options for compile
        :return:
        """
        offset_mapping = doc.compile(**kwargs)

        texts = {}
        layers = {}
        schema = {}

        for txt in doc.texts.values():
            texts[txt.name] = txt.compile(offset_mapping[txt.name][1])

        node_getter = Node.get

        # Encode types
        for k, v in doc.layers.items():
            typeschema = {}

            prop_encoder = {}
            for field, fieldtype in v.schema.fields.items():
                typeschema[field] = fieldtype.encode()

                if fieldtype.typename == DataTypeEnum.EXT:
                    prop_encoder[field] = XmlCodec._ext_encoder
                elif fieldtype.typename == DataTypeEnum.SPAN:
                    prop_encoder[field] = XmlCodec._span_encoder(offset_mapping[fieldtype.options["context"]][0])
                else:
                    prop_encoder[field] = XmlCodec.encoders[fieldtype.typename]

            schema[k] = typeschema

            layer_nodes = []

            for n in v:
                node_values = {}
                for field, encoder in prop_encoder.items():
                    res = node_getter(n, field, None)
                    if res is not None:
                        node_values[field] = encoder(res)

                layer_nodes.append(node_values)

            layers[k] = layer_nodes

        return texts, schema, layers

    @staticmethod
    def encode_utf8string(doc: Document, **kwargs):
        """
        Encode docria document into an XML string.

        :param doc: docria document
        :param kwargs: additional options, see XmlCodec.encode_tree and XmlCodec.encode_intermediate for options.
        :return:
        """
        tree = XmlCodec.encode_tree(doc, **kwargs)
        from io import BytesIO
        raw_output = BytesIO()
        tree.write(raw_output, encoding="utf-8")
        return raw_output.getvalue().decode("utf-8")

    @staticmethod
    def encode_tree(doc: Document, verbose=False, verbose_node_spans=False, document_id="", **kwargs)-> "xml.etree.ElementTree.ElementTree":
        """
        Encodes a docria document into an XML representation.

        :param doc: docria document
        :param verbose: add extra attributes to the XML data for readability and simpler tooling
        :param verbose_node_spans: add extra nodes for each node, materializing the span for readability
        :param document_id: the global unique document id
        :param kwargs: additional optoins, see XmlCodec.encode_intermediate for options
        :return:
        """
        import xml.etree.ElementTree as ET
        texts, schema, layers = XmlCodec.encode_intermediate(doc, **kwargs)

        if document_id != "":
            document_node = ET.Element("document", {"{http://www.w3.org/XML/1998/namespace}id": document_id})
            prefix = document_id + "."
        else:
            document_node = ET.Element("document")
            prefix = ""

        root = ET.ElementTree(element=document_node)

        prop_node = ET.SubElement(document_node, "props")
        for k, v in doc.props.items():
            ET.SubElement(prop_node, "prop", {"key": k, "value": str(v)})

        schema_node = ET.SubElement(document_node, "schema")
        for layer, schemadef in schema.items():
            layer_schema_node = ET.SubElement(schema_node, "define", {"layer": layer})
            for field, fielddef in schemadef.items():
                if isinstance(fielddef, dict):
                    field_node = ET.SubElement(layer_schema_node, "field", {"name": field, "type": fielddef["type"]})
                    for argk, argv in fielddef["args"].items():
                        ET.SubElement(field_node, "arg", {"key": argk, "value": str(argv)})
                else:
                    ET.SubElement(layer_schema_node, "field", {"name": field, "type": fielddef})

        texts_node = ET.SubElement(document_node, "texts")
        for textk, textv in texts.items():
            text_node = ET.SubElement(texts_node, "text", {"name": textk})
            seps_node = ET.SubElement(text_node, "sep")
            if verbose:
                pos = 0
                i = 0
                for entry in textv:
                    ET.SubElement(seps_node, "s", {"v": entry, "id": str(i), "start": str(pos), "stop": str(pos + len(entry))})
                    pos += len(entry)
                    i += 1

                ET.SubElement(text_node, "raw", {"value": "".join(textv)})
            else:
                for entry in textv:
                    ET.SubElement(seps_node, "s", {"v": entry})

        layers_node = ET.SubElement(document_node, "layers")
        for layerk, layerv in layers.items():
            layer_node = ET.SubElement(layers_node, "layer", {"name": layerk})
            for i, n in enumerate(layerv):
                array_entries = {}
                simple_entries = {}
                text_entries = {}
                embedded_entries = []
                for fk, fv in n.items():
                    if isinstance(fv, tuple):
                        offset_mapping = fv[0]  # type: Dict[int,int]
                        span = fv[1]  # type: TextSpan
                        embedded_entries.append(
                            ET.Element("d",
                                          {"name": fk,
                                           "from": str(offset_mapping[span.start]),
                                           "until": str(offset_mapping[span.stop])
                                          })
                        )
                        if verbose_node_spans:
                            text_entries[fk] = XmlCodec._string_encoder(fv)
                    elif isinstance(fv, NodeSpan):
                        embedded_entries.append(
                            ET.Element("d",
                                          {"name": fk,
                                           "from": fv.left.collection.name + "." + str(fv.left.i),
                                           "to": fv.left.collection.name + "." + str(fv.right.i)})
                        )
                    elif isinstance(fv, list):
                        array_entries[fk] = fv
                    elif isinstance(fv, str):
                        simple_entries[fk] = fv
                    else:
                        raise ValueError("Unsupported entry in output: %s, (%s)" % (repr(fv), type(fv)))

                simple_entries["{http://www.w3.org/XML/1998/namespace}id"] = prefix + layerk + "." + str(i)

                n_node = ET.SubElement(layer_node, "n", simple_entries)
                for ee in embedded_entries:
                    n_node.append(ee)

                for ak, av in array_entries.items():
                    array_node = ET.SubElement(n_node, "a", {"name": ak})
                    for av_entry in av:
                        ET.SubElement(array_node, "e", {"v": av_entry})

                if verbose_node_spans and len(text_entries) > 0:
                    for tk, tv in text_entries.items():
                        ET.SubElement(n_node, "t", {"key": tk, "value": tv})

        return root

