package org.embulk.filter.calcite.adapter.page;

import org.apache.calcite.linq4j.Enumerator;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

public class PageEnumerator
        implements Enumerator<Object[]>
{
    private final Schema schema;
    private final PageReader pageReader;

    private Object[] current;

    public PageEnumerator(Schema schema)
    {
        this.schema = schema;
        this.pageReader = new PageReader(schema);
        this.current = new Object[schema.getColumnCount()];
    }

    public void setPage(Page page)
    {
        this.pageReader.setPage(page);
    }

    @Override
    public Object[] current()
    {
        schema.visitColumns(new ColumnVisitor() {
            @Override
            public void booleanColumn(Column column)
            {
                // Embulk's boolean is converted into Java's boolean
                int i = column.getIndex();
                current[i] = pageReader.isNull(i) ? null : pageReader.getBoolean(i);
            }

            @Override
            public void longColumn(Column column)
            {
                // Embulk's long is converted into Java's long
                int i = column.getIndex();
                current[i] = pageReader.isNull(i) ? null : pageReader.getLong(i);
            }

            @Override
            public void doubleColumn(Column column)
            {
                // Embulk's double is converted into Java's double
                int i = column.getIndex();
                current[i] = pageReader.isNull(i) ? null : pageReader.getDouble(i);
            }

            @Override
            public void stringColumn(Column column)
            {
                // Embulk's string is converted into Java's string
                int i = column.getIndex();
                current[i] = pageReader.isNull(i) ? null : pageReader.getString(i);
            }

            @Override
            public void timestampColumn(Column column)
            {
                // Embulk's timestamp is converted into Java's long as unix timestamp TODO replace with timestamp with timezone
                int i = column.getIndex();
                current[i] = pageReader.isNull(i) ? null : pageReader.getTimestamp(i).getEpochSecond();
            }

            @Override
            public void jsonColumn(Column column)
            {
                // Embulk's json is converted into Java's string
                int i = column.getIndex();
                current[i] = pageReader.isNull(i) ? null : pageReader.getJson(i).toJson();
            }
        });

        return current;
    }

    @Override
    public boolean moveNext()
    {
        return pageReader.nextRecord();
    }

    @Override
    public void reset()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        if (pageReader != null) {
            pageReader.close();
        }
    }
}
