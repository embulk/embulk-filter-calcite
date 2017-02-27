package org.embulk.filter.calcite.adapter.page;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.embulk.spi.Schema;

import java.util.Map;

public class PageSchema
        extends AbstractSchema
{
    public static ThreadLocal<Schema> schema = new ThreadLocal<>();

    public PageSchema()
    {
        super();
    }

    @Override
    protected Map<String, Table> getTableMap()
    {
        return ImmutableMap.<String, Table>of("$PAGES", new PageTable(schema.get(), null));
    }
}
