package org.embulk.filter.calcite.adapter.page;

import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Factory that creates a {@link PageSchema}.
 * @see https://github.com/apache/calcite/blob/master/example/csv/src/main/java/org/apache/calcite/adapter/csv/CsvSchemaFactory.java
 */
public class PageSchemaFactory
        implements SchemaFactory
{
    public static final PageSchemaFactory INSTANCE = new PageSchemaFactory();

    private PageSchemaFactory()
    {
    }

    @Override
    public org.apache.calcite.schema.Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand)
    {
        return new PageSchema();
    }
}
