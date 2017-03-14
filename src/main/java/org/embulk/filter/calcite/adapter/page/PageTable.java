package org.embulk.filter.calcite.adapter.page;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;
import org.embulk.filter.calcite.PageConverter;
import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.Schema;

import java.util.ArrayList;
import java.util.List;

public class PageTable
        extends AbstractTable
        implements ScannableTable
{
    public static ThreadLocal<Page> page = new ThreadLocal<>();

    private final Schema schema;
    private final PageConverter valueMapper;
    private final RelProtoDataType protoRowType;

    PageTable(Schema schema, PageConverter valueMapper, RelProtoDataType protoRowType)
    {
        this.schema = schema;
        this.valueMapper = valueMapper;
        this.protoRowType = protoRowType;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory)
    {
        if (protoRowType != null) {
            return protoRowType.apply(typeFactory);
        }

        final List<RelDataType> types = new ArrayList<>(schema.getColumnCount());
        final List<String> names = new ArrayList<>(schema.getColumnCount());

        for (Column column : schema.getColumns()) {
            names.add(column.getName());
            PageFieldType type = PageFieldType.of(column.getType().getName());
            types.add(type.toType((JavaTypeFactory) typeFactory));
        }

        return typeFactory.createStructType(Pair.zip(names, types));
    }

    public Enumerable<Object[]> scan(DataContext root)
    {
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator()
            {
                PageEnumerator enumerator = new PageEnumerator(schema, valueMapper);
                if (page.get() != null) {
                    enumerator.setPage(page.get());
                }
                return enumerator;
            }
        };
    }
}
