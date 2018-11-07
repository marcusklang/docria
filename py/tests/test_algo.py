from docria import Document, DocumentIO
from docria.algorithm import span_translate


def test_span_remap():
    doc = Document()
    raw_text = doc.add_text("raw", "0123456789012345678901234567890")
    clean_text = doc.add_text("clean", "012345012")

    mapping_layer = doc.add_layer("mapping", raw=raw_text.spantype, clean=clean_text.spantype)
    mapping_layer.add(raw=raw_text[10:15], clean=clean_text[0:5])
    mapping_layer.add(raw=raw_text[20:22], clean=clean_text[5:7])
    mapping_layer.add(raw=raw_text[25:27], clean=clean_text[7:9])

    data_layer = doc.add_layer("data", raw=raw_text.spantype, clean=clean_text.spantype)
    data_layer.add(clean=clean_text[0:3])
    data_layer.add(clean=clean_text[5:9])
    data_layer.add(clean=clean_text[4:7])
    data_layer.add(clean=clean_text[5:9])

    span_translate(doc, "mapping", ("raw", "clean"), "data", ("clean", "raw"))

    assert "raw" in data_layer[0]
    assert "raw" in data_layer[1]
    assert "raw" in data_layer[2]
    assert "raw" in data_layer[3]

    assert data_layer[0]["raw"].start == 10 and data_layer[0]["raw"].stop == 13
    assert data_layer[1]["raw"].start == 20 and data_layer[1]["raw"].stop == 27
    assert data_layer[2]["raw"].start == 14 and data_layer[2]["raw"].stop == 22
    assert data_layer[3]["raw"].start == 20 and data_layer[3]["raw"].stop == 27
