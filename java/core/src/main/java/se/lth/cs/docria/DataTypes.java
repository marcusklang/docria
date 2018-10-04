package se.lth.cs.docria;

public class DataTypes {
    public static final DataType NULL = new DataType.Builder(DataTypeName.NULL).build();
    public static final DataType BOOLEAN = new DataType.Builder(DataTypeName.BOOL).build();
    public static final DataType INT_32 = new DataType.Builder(DataTypeName.INT).build();
    public static final DataType INT_64 = new DataType.Builder(DataTypeName.LONG).build();
    public static final DataType FLOAT_64 = new DataType.Builder(DataTypeName.DOUBLE).build();
    public static final DataType STRING = new DataType.Builder(DataTypeName.STRING).build();
    public static final DataType BINARY = new DataType.Builder(DataTypeName.BINARY).build();
    public static DataType span(String context) {
        return new DataType.Builder(DataTypeName.SPAN).addArg("context", context).build();
    }
}
