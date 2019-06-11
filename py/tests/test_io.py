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
from docria.model import Document, DataTypes as T
from docria.storage import MsgpackDocumentWriter, _BoundaryWriter, _BoundaryReader, MsgpackDocumentReader
import re
import os

def test_io():
    # Stupid tokenizer
    tokenizer = re.compile(r"[a-zA-Z]+|[0-9]+|[^\s]")

    doc = Document()
    main_text = doc.add_text("main", "This code was written in Lund, Sweden.")
    #                                 01234567890123456789012345678901234567
    #                                 0         1         2         3

    token = doc.add_layer("token", id=T.int32, text=main_text.spantype, head=T.noderef("token"))

    i = 1
    for m in tokenizer.finditer(str(main_text)):
        token.add(id=i, text=main_text[m.start():m.end()])
        i += 1

    token[2]["head"] = token[0]

    with MsgpackDocumentWriter(_BoundaryWriter(open("test.docria", "wb"))) as writer:
        writer.write(doc)

    del main_text
    del token
    del doc

    with MsgpackDocumentReader(_BoundaryReader(open("test.docria", "rb"))) as reader:
        doc = next(reader)  # type:

    os.unlink("test.docria")

test_io()