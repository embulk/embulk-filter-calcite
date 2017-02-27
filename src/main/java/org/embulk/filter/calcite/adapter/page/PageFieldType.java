package org.embulk.filter.calcite.adapter.page;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

enum PageFieldType
{
    STRING(String.class, "string"),
    BOOLEAN(boolean.class, "boolean"),
    LONG(long.class, "long"),
    DOUBLE(double.class, "double"),
    TIMESTAMP(org.embulk.spi.time.Timestamp.class, "timestamp");

    private final Class clazz;
    private final String simpleName;

    private static final Map<String, PageFieldType> MAP = new HashMap<String, PageFieldType>();

    static {
        for (PageFieldType value : values()) {
            MAP.put(value.simpleName, value);
        }
    }

    PageFieldType(Class clazz, String simpleName) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }

    public RelDataType toType(JavaTypeFactory typeFactory) {
        return typeFactory.createJavaType(clazz);
    }

    public static PageFieldType of(String typeString) {
        return MAP.get(typeString);
    }
}
