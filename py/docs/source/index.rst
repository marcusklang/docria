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

To install the python version:

.. code-block:: bash

   pip install docria


The first steps
---------------
.. code-block:: python

   from docria.model import Document, DataTypes as T
   import re

   # Stupid tokenizer
   tokenizer = re.compile(r"[a-zA-Z]+|[0-9]+|[^\s]")
   starts_with_uppercase = re.compile(r"[A-Z].*")

   doc = Document()

   # Create a new text context called 'main' with the text 'This code was written in Lund, Sweden.'
   doc.maintext = "This code was written in Lund, Sweden.")
   #               01234567890123456789012345678901234567
   #               0         1         2         3

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
     token_node["uppercase"] = starts_with_uppercase.fullmatch(m.text()) is not None
     i += 1

   # Reading nodes
   for tok in tokens:
      print(tok["text"])

   # Filtering, only uppercase tokens
   for tok in tokens[tokens["uppercase"] == True]:
      print(tok["text"])

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


API Reference
=============
.. autosummary::
    :toctree: generated

   docria.model
   docria.algorithm
   docria.codec
   docria.storage
   docria.printout


.. include:: generated/docria.model.rst
.. include:: generated/docria.algorithm.rst
.. include:: generated/docria.codec.rst
.. include:: generated/docria.storage.rst
.. include:: generated/docria.printout.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

