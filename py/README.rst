Docria (Python)
===============

Semi-structured strongly typed document storage model for Python 3+

---------------

Overview
--------

The document model consists of the following concepts:

 * **Document**: The overall container for everything (all nodes, layers, texts must be contained within)
 * **Document fields**: a single dictionary per document to store metadata in.
 * **Text**: The basic text representation, a wrapped string to track spans.
 * **Text Spans**: Subsequence of a string, can always be converted into a hard string by using str(span)
 * **Layer**: Collection of nodes
 * **Layer Schema**: Definition of field names and types when document is serialized
 * **Node**: Single node with zero or more fields with values
 * **Node fields**: Key, value pairs.

All parts of the document are accessible in three properties:

.. code-block:: python

    from docria.model import Document

    doc = Document()
    doc.props  # The Document metadata dictionary
    doc.layers # The layer dictionary, name of layer to collection
    doc.texts  # The texts dictionary.


Example of usage
----------------

.. code-block:: python

    :name How to create a document and insert nodes

    from docria.model import Document, DataTypes as T
    import re
    # Stupid tokenizer
    tokenizer = re.compile(r"[a-zA-Z]+|[0-9]+|[^\s]")

    doc = Document()

    # Create a new text context called 'main' with the text 'This code was written in Lund, Sweden.'
    main_text = doc.add_text("main", "This code was written in Lund, Sweden.")
    #                                 01234567890123456789012345678901234567
    #                                 0         1         2         3

    # Create a new layer with fields: id, text and head.
    #
    # Fields:
    #   id is an int32
    #   text is a span from context 'main'
    #   head is a node reference into the token layer (the layer we are creating)
    #
    tokens = doc.add_layer("token", id=T.int32, text=main_text.spantype, head=T.noderef("token"))

    # Adding nodes: Solution 1
    i = 1
    token_zero = None
    token_two = None
    for m in tokenizer.finditer(str(main_text)):
        token_node = tokens.add(id=i, text=main_text[m.start():m.end()])
        if i == 0:
            token_zero = token_node
        elif i == 2:
            token_two = token_node

        i += 1

    token_two["head"] = token_zero

    # Solution 2: If adding many nodes
    token_list = []

    i = 1
    for m in tokenizer.finditer(str(main_text)):
        # This token is dangling, and is not attached until add_many
        token = Node({"id": i, "text": main_text[m.start():m.end()]}))
        token_list.append(token)
        i += 1

    token_list[2]["head"] = token_list[0]
    tokens.add_many(token_list)

Document I/O
------------

In ``docria.storage`` there is a DocumentIO class which provides factory methods to create readers and writers.

.. code-block python
    :name How to create file writer and reader

    from docria.storage import DocumentIO

    with DocumentIO.write("output-file.docria") as docria_writer:
        for doc in documents:
            docria_writer.write(doc)


    with DocumentIO.read("output-file.docria") as docria_reader:
        for doc in docria_reader:
            # Do something with doc, which is a document
            pass

Notes
-----

Use regular object references when referring to a node.

The settings used for pretty printing is controlled by ``docria.printout.options``.

By convention pretty printing will output [layer name]#[internal id] where the internal id can be used to get the node.
However, this id is only guaranteed to be static if the layer is not changed, if changed it is invalid.

