.. docria documentation master file, created by
   sphinx-quickstart on Mon Jun 10 10:29:05 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Docria
======
.. contents:: :local:

.. toctree::
   :maxdepth: 3

   :caption: Contents:

Docria provides a hypergraph document model implementation with a focus on \
NLP (Natural Language Processing) applications.

Docria provides:

 * In-memory object representations in Python and Java
 * Binary serialization format based on MessagePack
 * File formats optimized for storing and accessing millions of documents locally and in a cluster context

Quickstart
==========

To install the PyPI version:

.. code-block:: bash

   pip install docria

To install the development version:

.. code-block:: bash

   git clone https://github.com/marcusklang/docria
   cd docria
   pip install -e .


The first steps
---------------
.. code-block:: python

   from docria.model import Document, DataTypes as T
   import regex as re

   # Stupid tokenizer
   tokenizer = re.compile(r"[a-zA-Z]+|[0-9]+|[^\s]")
   starts_with_uppercase = re.compile(r"[A-Z].*")

   doc = Document()

   # Create a new text context called 'main' with the text 'This code was written in Lund, Sweden.'
   doc.maintext = "This code was written in Lund, Sweden."
   #               01234567890123456789012345678901234567
   #               0         1         2         3
   main_text = doc.maintext

   # Create a new layer with fields: id, text and head.
   #
   # Fields:
   #   id is an int32
   #   uppercase is a boolean indicating if the token is uppercase
   #   text is a textspan from context 'main'
   #
   tokens = doc.add_layer("token", id=T.int32(), uppercase=T.bool(), text=T.span())

   # Adding nodes: Solution 1
   i = 0
   for m in tokenizer.finditer(str(main_text)):
       token_node = tokens.add(id=i, text=main_text[m.start():m.end()])

       # Check if it is uppercase
       token_node["uppercase"] = starts_with_uppercase.fullmatch(m[0]) is not None
       i += 1

   # Reading nodes
   for tok in tokens:
       print(tok["text"])

   # Filtering, only uppercase tokens
   for tok in tokens[tokens["uppercase"] == True]:
       print(tok["text"])

Concepts
--------

The document model consists of the following concepts:

 * **Document**: The overall container for everything (all nodes, layers, texts must be contained within)
 * **Document properties**: a single dictionary per document to store metadata in.
 * **Text**: The basic text representation, a wrapped string to track spans.
 * **Text Spans**: Subsequence of a string, can always be converted into a hard string by using str(span)
 * **Node Spans**: Start and stop node in a layer which will produce a sequence of nodes.
 * **Layer**: Collection of nodes
 * **Layer Schema**: Definition of field names and types when document is serialized
 * **Node**: Single node with zero or more fields with values
 * **Node fields**: Key, value pairs.

.. code-block:: python

    from docria.model import Document

    doc = Document()
    doc.maintext # alias to doc.text["main"] with special support for
                 # creating a main text via doc.maintext = "string"

    doc.props  # Document metadata dictionary
    doc.layers # Layer dictionary, layer name to node layer collection
    doc.layer  # Alias to above
    doc.texts  # Text dictionary.
    doc.text   # Alias to above


Examples
========

Reading document collections
----------------------------
.. code-block:: python

   from docria.storage import MsgpackDocumentReader
   from docria.codec import MsgpackDocument

   with MsgpackDocumentReader(open("path_to_your_docria_file.docria", "rb")) as reader:
      for rawdoc in reader:
         # rawdoc is of type MsgpackDocument
         doc = rawdoc.document() #  type: docria.Document

         # Print the schema
         doc.printschema()

         for token in doc["token"]:
            # ... do something with the data contained within.
            pass

   # You can use MsgpackDocumentReader as a normal instance
   # and manually use .close() when done or on the GC to eat it up.

The principle is mostly the same with :class:~`docria.storage.TarMsgpackReader` with the
exception it expects a filepath, not a filelike object.

Writing document collections
----------------------------
.. code-block:: python

   from docria.storage import MsgpackDocumentReader
   from docria.codec import MsgpackDocument

   with MsgpackDocumentWriter(open("path_to_your_docria_file.docria", "wb")) as writer:
      # using the previous doc in "The first steps"
      writer.write(doc)

   # Rewriting or filtering
   with MsgpackDocumentWriter(open("path_to_your_output_docria_file.docria", "wb")) as writer:
      with MsgpackDocumentReader(open("path_to_your_input_docria_file.docria", "rb")) as reader:
         for rawdoc in reader:
            writer.write(rawdoc)  # this is decompression and memory copy of the raw data

The principle is mostly the same with :class:~`docria.storage.TarMsgpackWriter` with the
exception it expects a filepath, not a filelike object.

Reading and writing documents to bytes
--------------------------------------
.. code-block:: python

    from docria.codec import MsgpackCodec, MsgpackDocument

    binarydata = bytes()  # from any location
    binarydata = io.BytesIO()  # or

    # To decode a msgpack document into a document
    msgdoc = MsgpackDocument(binarydata)
    doc = msgdoc.document()  # type: docria.model.Document

    # To encode a document into a msgpack document
    msgdoc = MsgpackDocument(doc)
    binarydata = msgdoc.binary()  # type: bytes

    # Access data without a full deserialization
    rawdoc = MsgpackDocument(binarydata)
    rawdow.properties()  # Document metadata as dictionary

    # Document texts, dictionary name to list of strings
    # (each segment which potentially has annotation) which can be joined to get the full text.
    rawdoc.texts()

    schema = rawdoc.schema() # advanced access to the contents of this document, lists layers and fields.

    doc = rawdoc.document() # full document deserialization

Layer and field query
---------------------
.. code-block:: python

   from docria import Document, DataTypes as T, NodeSpan, NodeList

   doc = Document()
   doc.maintext = "Lund is a city in Sweden."
   #               0123456789012345678901234
   #               0         1         2

   # Only ordered layers exist in docria, this means all nodes are added sequentially.
   # T.span() is equivalent to T.span("main") which referes to the main text
   token_layer = doc.add_layer("token", part_of_speech=T.string(), text=T.span(), head=T.noderef("token"))

   # Annotation output by CoreNLP 3.9.2 and Basic dependencies
   # We set node references later.
   first = token_layer.add(part_of_speech="NNP", text=doc.maintext[0:4])
   token_layer.add(part_of_speech="VBZ", text=doc.maintext[5:7])
   token_layer.add(part_of_speech="DT", text=doc.maintext[8:9])
   token_layer.add(part_of_speech="NN", text=doc.maintext[10:14])
   token_layer.add(part_of_speech="IN", text=doc.maintext[15:17])
   token_layer.add(part_of_speech="NNP", text=doc.maintext[18:24])
   last = token_layer.add(part_of_speech=".", text=doc.maintext[24:])

   # Create a node span and convert into a list
   sent_tokens = NodeSpan(first, last).to_list()

   # When setting heads, no validation takes place.
   sent_tokens[0]["head"] = token_layer[3] # head = city
   sent_tokens[1]["head"] = token_layer[3] # head = city
   sent_tokens[2]["head"] = token_layer[3] # head = city
   sent_tokens[4]["head"] = token_layer[5] # head = Sweden
   sent_tokens[5]["head"] = token_layer[3] # head = city
   sent_tokens[6]["head"] = token_layer[3] # head = city

   sent_tokens.validate() # We can manually initiate validate for these nodes to fail faster.

   # This first query finds all roots by checking if the head is None, and finally picks the first one.
   first_root = token_layer[token_layer["head"].is_none()].first()

   # This second query finds all nodes with the head equal to first_root
   tokens_with_head_first_root = token_layer[token_layer["head"] == first_root]

   # Then we print tokens in layer order from matching token to including root token
   for tok in tokens_with_head_first_root:
       # iter_span is invariant to order, it will always produce low id to high id.
       print(NodeList(first_root.iter_span(tok))["text"].to_list())

Change presentation settings
----------------------------
The settings used for pretty printing is controlled by the global variable :py:const:`docria.printout.options`
which is a :py:class:`docria.printout.PrintOptions`.

By convention pretty printing will output [layer name]#[internal id] where the internal id can be used to get the node.
However, this id is only guaranteed to be static if the layer is not changed, if changed it is invalid.

For references in general use the Node object.

.. include:: api.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

