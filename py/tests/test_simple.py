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
from docria.model import Document, DataTypes as T, SchemaValidationError, NodeSpan
from docria.codec import MsgpackCodec, JsonCodec
import re
import base64


def test_primary():
    """Test basic layer creation and node creation."""
    # Stupid tokenizer
    tokenizer = re.compile(r"[a-zA-Z]+|[0-9]+|[^\s]")

    doc = Document()
    main_text = doc.add_text("main", "This code was written in Lund, Sweden.")
    #                                 01234567890123456789012345678901234567
    #                                 0         1         2         3

    token = doc.add_layer("token", text=main_text.spantype)
    for m in tokenizer.finditer(str(main_text)):
        token.add(text=main_text[m.start():m.end()])

    named_entity = doc.add_layer("named_entity", text=main_text.spantype, cls=T.string)
    named_entity.add(text=main_text[25:29], cls="GPE")
    named_entity.add(text=main_text[31:37], cls="GPE")

    assert str(main_text) == "This code was written in Lund, Sweden."
    assert " ".join(map(str, map(lambda tok: tok["text"], list(token)))) == "This code was written in Lund , Sweden ."
    assert " ".join(map(str, map(lambda tok: tok["text"], list(named_entity)))) == "Lund Sweden"
    assert " ".join(map(str, map(lambda tok: tok["cls"], list(named_entity)))) == "GPE GPE"

    return doc


def create_doc():
    """Test basic layer creation and node creation."""
    # Stupid tokenizer
    tokenizer = re.compile(r"[a-zA-Z]+|[0-9]+|[^\s]")

    doc = Document()
    main_text = doc.add_text("main", "This code was written in Lund, Sweden.")
    #                                 01234567890123456789012345678901234567
    #                                 0         1         2         3

    token = doc.add_layer("token", text=main_text.spantype)
    for m in tokenizer.finditer(str(main_text)):
        token.add(text=main_text[m.start():m.end()])

    named_entity = doc.add_layer("named_entity", text=main_text.spantype, cls=T.string)
    named_entity.add(text=main_text[25:29], cls="GPE")
    named_entity.add(text=main_text[31:37], cls="GPE")

    return doc


def test_nodespan():
    doc = create_doc()
    geoloc = doc.add_layer("geolocations", tokens=T.nodespan("token"))
    tokens = list(doc["token"][doc["token"]["text"].covered_by(25, 37)])
    n = geoloc.add(tokens=NodeSpan(tokens[0], tokens[-1]))
    assert " ".join(map(lambda tok: str(tok["text"]), n["tokens"])) == "Lund , Sweden"

    redoc = MsgpackCodec.decode(MsgpackCodec.encode(doc))
    geoloc = redoc["geolocations"]
    assert " ".join(map(lambda tok: str(tok["text"]), geoloc[0]["tokens"])) == "Lund , Sweden"


def test_retain():
    doc = create_doc()

    tokens = doc["token"].to_list()[[1, 5, 7]]
    assert str(tokens[0]["text"]) == "code"
    assert str(tokens[1]["text"]) == "Lund"
    assert str(tokens[2]["text"]) == "Sweden"

    doc["token"].retain(tokens)

    assert "code Lund Sweden" == " ".join(map(lambda n: str(n["text"]), doc["token"]))
    assert len(doc["token"]) == 3


def test_remove():
    doc = create_doc()
    doc["token"].remove(doc["token"][1])
    assert "This was written in Lund , Sweden ." == " ".join(map(lambda n: str(n["text"]), doc["token"]))

    toks = doc["token"].to_list()

    doc["token"].remove(toks[[0, 1, 2]])
    assert "in Lund , Sweden ." == " ".join(map(lambda n: str(n["text"]), doc["token"]))

    doc["token"].remove(toks[-1])
    assert "in Lund , Sweden" == " ".join(map(lambda n: str(n["text"]), doc["token"]))


def test_primary_msgpack():
    """Test basic layer creation and node creation with msgpack serialization roundtrip"""
    doc = MsgpackCodec.decode(MsgpackCodec.encode(test_primary()))

    main_text = doc.texts["main"]
    token = doc.layers["token"]
    named_entity = doc.layers["named_entity"]

    repr = MsgpackCodec.encode(doc)

    assert str(main_text) == "This code was written in Lund, Sweden."
    assert " ".join(map(str, map(lambda tok: tok["text"], list(token)))) == "This code was written in Lund , Sweden ."
    assert " ".join(map(str, map(lambda tok: tok["text"], list(named_entity)))) == "Lund Sweden"
    assert " ".join(map(str, map(lambda tok: tok["cls"], list(named_entity)))) == "GPE GPE"


def test_primary_json():
    """Test basic layer creation and node creation with json serialization roundtrip"""
    doc = JsonCodec.decode(JsonCodec.encode(test_primary()))

    main_text = doc.texts["main"]
    token = doc.layers["token"]
    named_entity = doc.layers["named_entity"]

    assert str(main_text) == "This code was written in Lund, Sweden."
    assert " ".join(map(str, map(lambda tok: tok["text"], list(token)))) == "This code was written in Lund , Sweden ."
    assert " ".join(map(str, map(lambda tok: tok["text"], list(named_entity)))) == "Lund Sweden"
    assert " ".join(map(str, map(lambda tok: tok["cls"], list(named_entity)))) == "GPE GPE"


def test_java_interaction():
    binary_data = base64.standard_b64decode(
        "RE1fMQGAkqxuYW1lZF9lbnRpdHmldG9rZW4Co2Nsc8Kjc3RypHRleHTDpHNwYW6Bp2NvbnRleHSkbWFpbgGkdGV4dMOkc3BhboGnY29udGV4d"
        "KRtYWluPIGkbWFpbp+kVGhpc6EgpGNvZGWhIKN3YXOhIKd3cml0dGVuoSCiaW6hIKRMdW5koSyhIKZTd2VkZW6hLhECwpKjR1BFo0dQRcKUCg"
        "sNDhcJwtwAEgABAgMEBQYHCAkKCwsMDQ4ODw==")

    doc = MsgpackCodec.decode(binary_data)
    assert " ".join(map(lambda tok: str(tok["text"]), doc.layers["token"])) == "This code was written in Lund , Sweden ."
    assert " ".join(map(lambda ne: str(ne["text"]), doc.layers["named_entity"])) == "Lund Sweden"


def test_schema():
    doc = Document()

    text = doc.add_text("main", "Text")
    token = doc.add_layer("token", text=text.spantype)
    token.add(undeclared_field="Test")

    try:
        MsgpackCodec.encode(doc)
        assert False
    except SchemaValidationError:
        assert True

    try:
        JsonCodec.encode(doc)
        assert False
    except SchemaValidationError:
        assert True


def test_text():
    doc = Document()
    doc.add_text("main", "This code was written in Lund, Sweden")
    #                     01234567890123456789012345678901234567
    #                     0         1         2         3

    main_text = doc.text["main"]
    assert str(main_text) == "This code was written in Lund, Sweden"
    assert str(main_text[:]) == "This code was written in Lund, Sweden"
    assert str(main_text[:4]) == "This"
    assert str(main_text[31:]) == "Sweden"
    assert str(main_text[14:21]) == "written"
    assert str(main_text[14, 21]) == "written"
    assert str(main_text[-4:-2]) == "ed"
    assert str(main_text[:-2]) == "This code was written in Lund, Swed"
    assert main_text[0] == "T"
    assert main_text[-1] == "n"

    n = 0
    output = []
    for ch in main_text:
        n += 1
        output.append(ch)

    assert "".join(output) == str(main_text)

    assert str(main_text[14:21][0:2]) == "wr"
    assert str(main_text[14:21][:2]) == "wr"
    assert str(main_text[14:21][2:5]) == "itt"
    assert str(main_text[14:21][2, 5]) == "itt"
    assert str(main_text[14:21][:-2]) == "writt"
    assert str(main_text[14:21][-2:]) == "en"
    assert str(main_text[14:21][:]) == "written"

    assert main_text[14:21][0] == "w"
    assert main_text[14:21][-1] == "n"


def test_field_add():
    doc = Document()
    maintext = doc.add_text("main", "Lund")
    token = doc.add_layer("token", id=T.int32, text=T.span(maintext))

    token.add(id=1, text=maintext[0:1])
    token.add(id=2, text=maintext[1:2])

    token.add_field("head", T.noderef(token))

    from functools import reduce
    assert reduce(lambda x, y: x and y, map(lambda x: "head" not in x, token))

    token.add_field("pos", T.string())
    assert reduce(lambda x, y: x and y, map(lambda x: "pos" in x and x["pos"] == "", token))

    token.add_field("is_upper", T.bool())
    assert reduce(lambda x, y: x and y, map(lambda x: "is_upper" in x and x["is_upper"] == False, token))


def test_typing():
    pass


def test_graph():
    pass
