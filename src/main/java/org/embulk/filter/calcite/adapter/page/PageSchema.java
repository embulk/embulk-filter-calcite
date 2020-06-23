package org.embulk.filter.calcite.adapter.page;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.embulk.spi.Schema;

public class PageSchema extends AbstractSchema {

    public static Schema schema;

    public PageSchema() {
        super();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        final HashMap<String, Table> tableMap = new HashMap<>();
        tableMap.put("$PAGES", new PageTable(schema, null));
        return Collections.unmodifiableMap(tableMap);
    }
}
