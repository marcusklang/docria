from docria import Document, DocumentIO
from docria.algorithm import span_translate, group_by_span, dominant_right
from docria import T


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


def test_group_by():
    doc = Document()
    raw_text = doc.add_text("raw", "0123456789012345678901234567890")

    tokens = doc.add_layer("token", id=T.int32, span=raw_text.spantype)
    anchors = doc.add_layer("anchor", id=T.int32, mention=raw_text.spantype)
    entities = doc.add_layer("entity", id=T.int32, entity=raw_text.spantype)

    tokens.add(id=1, span=raw_text[0:2])
    tokens.add(id=2, span=raw_text[3:5])
    tokens.add(id=3, span=raw_text[4:6])  # Overlapping, not typical but shall be supported
    tokens.add(id=4, span=raw_text[8:11])
    tokens.add(id=5, span=raw_text[11:12])
    tokens.add(id=6, span=raw_text[14:15])
    tokens.add(id=7, span=raw_text[17:17])

    anchors.add(id=8, mention=raw_text[0:6])  # tests [start_match, stop_match]
    anchors.add(id=9, mention=raw_text[1:6])  # tests [start_intersect, stop_cover]
    anchors.add(id=10, mention=raw_text[2:6])
    anchors.add(id=11, mention=raw_text[4:10])
    anchors.add(id=12, mention=raw_text[4:12])
    anchors.add(id=13, mention=raw_text[8:11])
    anchors.add(id=14, mention=raw_text[16:18])
    anchors.add(id=15, mention=raw_text[20:20])

    entities.add(id=16, entity=raw_text[4:6])
    entities.add(id=17, entity=raw_text[8:12])
    entities.add(id=18, entity=raw_text[0:15])

    groups = group_by_span(anchors,
                           layer_nodes={"tokens": tokens, "entity": entities},
                           group_span_field="mention",
                           layer_span_field={"tokens": "span", "entity": "entity"})

    id2group = {n["id"]: g for n, g in groups}

    assert set([tok["id"] for tok in id2group[8]["tokens"]]) == {1, 2, 3}
    assert set([tok["id"] for tok in id2group[9]["tokens"]]) == {1, 2, 3}
    assert set([tok["id"] for tok in id2group[10]["tokens"]]) == {2, 3}
    assert set([tok["id"] for tok in id2group[11]["tokens"]]) == {2, 3, 4}
    assert set([tok["id"] for tok in id2group[12]["tokens"]]) == {2, 3, 4, 5}
    assert set([tok["id"] for tok in id2group[13]["tokens"]]) == {4}
    assert set([tok["id"] for tok in id2group[14]["tokens"]]) == {7}
    assert set([tok["id"] for tok in id2group[15]["tokens"]]) == set()

    groups = group_by_span(anchors,
                           resolution="cover",
                           layer_nodes={"tokens": tokens, "entity": entities},
                           include_empty_groups=False,
                           group_span_field="mention",
                           layer_span_field={"tokens": "span", "entity": "entity"})

    id2group = {n["id"]: g for n, g in groups}

    assert set([tok["id"] for tok in id2group[8]["tokens"]]) == {1, 2, 3}
    assert set([tok["id"] for tok in id2group[9]["tokens"]]) == {2, 3}
    assert set([tok["id"] for tok in id2group[10]["tokens"]]) == {2, 3}
    assert set([tok["id"] for tok in id2group[11]["tokens"]]) == {3}
    assert set([tok["id"] for tok in id2group[12]["tokens"]]) == {3, 4, 5}
    assert set([tok["id"] for tok in id2group[13]["tokens"]]) == {4}
    assert set([tok["id"] for tok in id2group[14]["tokens"]]) == {7}
    assert 15 not in id2group


def test_dominant_right():
    segments = [
        (0, 3, 0),
        (1, 4, 1),
        (0, 1, 2),
        (0, 6, 3),
        (4, 5, 4),
        (8, 10, 5),
        (11, 15, 6),
        (14, 16, 7),
        (20, 25, 8),
        (19, 21, 9)]

    ids = dominant_right(segments)

    assert len(ids) == 4
    assert 3 in ids
    assert 5 in ids
    assert 6 in ids
    assert 8 in ids

