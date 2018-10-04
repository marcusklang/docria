package se.lth.cs.docria;

import java.util.TreeMap;

public enum DataTypeName {
    NULL("nil"),
    INT("i32"),
    LONG("i64"),
    DOUBLE("f64"),
    BOOL("i1"),
    STRING("str"),
    BINARY("bin"),
    SPAN("span"),
    NODE("noderef"),
    NODE_ARRAY("noderef_array");

    private final String name;

    private DataTypeName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    private static final TreeMap<String, DataTypeName> typeNames = new TreeMap<>();
    static {
        for (DataTypeName dataTypeName : DataTypeName.values()) {
            typeNames.put(dataTypeName.name, dataTypeName);
        }
    }

    public static DataTypeName from(String typeName) {
        return typeNames.get(typeName);
    }
}
