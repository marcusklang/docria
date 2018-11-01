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
from docria.model import TextSpan, Document, TEnum, NodeLayerSchema, String2DataType, DataType, Node, ExtData
import msgpack
import json
import logging
from io import BytesIO, IOBase
from typing import List, Dict, Tuple
from base64 import standard_b64decode


class DataError(Exception):
    """Serialization/Deserialization failure"""
    def __init__(self, message):
        super().__init__(message)


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
        TEnum.SPAN: codec_encode_span
    }

    @staticmethod
    def encode(doc: "Document", doc_encoder, **kwargs):
        doc.compile(**kwargs)

        texts = {}
        types = {}
        types_num_nodes = {}
        schema = {}

        for txt in doc.texts.values():
            txt._gc()
            texts[txt.name] = txt._compile()

        node_getter = Node.get

        # Encode types
        for k, v in doc.layers.items():
            propfields = {}
            typeschema = {}
            for field, fieldtype in v.schema.fields.items():
                typeschema[field] = fieldtype.encode()

                propvalues = []
                encoder = Codec.encoders[fieldtype.typename]
                if fieldtype.typename == TEnum.EXT:
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
                else:
                    for n in v:
                        encoder(propvalues, node_getter(n, field, None))

                propfields[field] = propvalues

            types_num_nodes[k] = v.num

            schema[k] = typeschema
            types[k] = propfields

        return texts, types, types_num_nodes, schema


class JsonCodec:
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
                if typedef.typename == TEnum.SPAN:
                    decoder = span_field(doc.texts[typedef.options["context"]], text2offsets[typedef.options["context"]])

                if typedef.typename == TEnum.EXT:
                    decoder = ext_field

                coldata = docobj["types"][typename][col]
                decoder(coldata)

        # Insert layers
        for typename in schema.keys():

            # Create Node Type
            nt = NodeLayerSchema(typename)
            for col, typedef in schema[typename]:
                if typedef.typename == TEnum.NODEREF:

                    # Replace int placeholds with an actual node reference.
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            n[col] = target_nodes[n[col]]
                elif typedef.typename == TEnum.NODEREF_MANY:
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


class MsgpackDocument:
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

    def properties(self, *props):
        self._parse_state(1)
        self.rawdata.seek(self._prop[0])
        unpacker = msgpack.Unpacker(self.rawdata, raw=False)
        return MsgpackCodec.decode_property(unpacker, *props)

    def schema(self):
        self._parse_state(2)
        return self._schema[0], self._schema[1]

    def texts(self, *texts):
        self._parse_state(2)
        self.rawdata.seek(self._texts[0])
        unpacker = msgpack.Unpacker(self.rawdata, raw=False)
        return MsgpackCodec.decode_texts(unpacker, *texts)

    def document(self, *layers, **kwargs):
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

        MsgpackCodec.commit_layers(doc, types, schema, all_nodes)
        return doc


class MsgpackDocumentExt(ExtData):
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

            textobj = doc.add_text(textname, fulltext)
            text2offsets[textname] = textobj.initialize_offsets(offsets)

        return text2offsets

    @staticmethod
    def decode_layer(unpacker, doc, typename, text2offsets, layerschema, *fields, **kwargs):
        num_nodes = next(unpacker)
        nodes = [Node() for _ in range(num_nodes)]

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
                if typedef.typename == TEnum.SPAN:
                    decoder = span_field(
                        doc.texts.get(typedef.options["context"], None),
                        text2offsets.get(typedef.options["context"], None))
                elif typedef.typename == TEnum.EXT:
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
    def commit_layers(doc, types, schema, all_nodes):
        # Insert layers
        for typename in types:

            # Create Node Type
            nt = NodeLayerSchema(typename)
            for col, typedef in schema[typename]:
                if typedef.typename == TEnum.NODEREF:

                    # Replace int placeholds with an actual node reference.
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            n[col] = target_nodes[n[col]]
                elif typedef.typename == TEnum.NODEREF_MANY:
                    # Replace int placeholds with an actual node reference.
                    target_type = typedef.options["layer"]
                    target_nodes = all_nodes[target_type]

                    for n in all_nodes[typename]:
                        if col in n:
                            n[col] = [target_nodes[n_item] for n_item in n[col]]

                nt.add(col, typedef)

            layer = doc.add_layer(nt)
            layer.unsafe_initialize(all_nodes[typename])

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
            all_nodes[typename] = MsgpackCodec.decode_layer(unpacker, doc, typename, text2offsets, layerschema, **kwargs)

        MsgpackCodec.commit_layers(doc, types, schema, all_nodes)
        return doc

