package org.embulk.filter.calcite.adapter.page;

import java.util.ArrayList;
import java.util.List;

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

/**
 * Base class for table that reads Pages.
 */
public class PageTable
        extends AbstractTable
        implements ScannableTable {

    public static ThreadLocal<PageConverter> pageConverter = new ThreadLocal<>();
    public static ThreadLocal<Page> page = new ThreadLocal<>();

    private final Schema schema;
    private final RelProtoDataType protoRowType;

    // Creates a {@code PageTable} object.
    PageTable(Schema schema, RelProtoDataType protoRowType) {
        this.schema = schema;
        this.protoRowType = protoRowType;
    }

    /**
     * Returns a {@code RelDataType} by a given {@code RelDataTypeFactory}.
     *
     * @param typeFactory a factory object to create {code RelDataType}s.
     */
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
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

    /**
     * Creates and returns a {@code Enumerable} object to read a {@code Page} object.
     *
     * @param root a {@code DataContext} object that can be used during scanning a {@code Page}
     *             object.
     * @return a {@code Enumerable} object
     */
    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                PageEnumerator enumerator = new PageEnumerator(schema, pageConverter.get());
                if (page.get() != null) {
                    enumerator.setPage(page.get());
                }
                return enumerator;
            }
        };
    }
}
