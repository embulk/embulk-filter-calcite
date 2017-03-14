package org.embulk.filter.calcite.adapter.page;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

enum PageFieldType
{
    STRING(String.class, "string"),
    BOOLEAN(Boolean.class, Boolean.TYPE.getSimpleName()),
    LONG(Long.class, Long.TYPE.getSimpleName()),
    DOUBLE(Double.class, Double.TYPE.getSimpleName()),
    TIMESTAMP(java.sql.Timestamp.class, "timestamp");

    private static final Map<String, PageFieldType> MAP = new HashMap<>();

    static
    {
        for (PageFieldType value : values()) {
            MAP.put(value.simpleName, value);
        }
    }

    private final Class clazz;
    private final String simpleName;

    PageFieldType(Class clazz, String simpleName)
    {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }

    public RelDataType toType(JavaTypeFactory typeFactory)
    {
        return typeFactory.createJavaType(clazz);
    }

    public static PageFieldType of(String typeString)
    {
        return MAP.get(typeString);
    }
}
