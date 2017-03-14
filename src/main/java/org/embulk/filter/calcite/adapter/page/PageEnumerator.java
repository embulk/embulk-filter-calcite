package org.embulk.filter.calcite.adapter.page;

import org.apache.calcite.linq4j.Enumerator;
import org.embulk.filter.calcite.PageConverter;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

public class PageEnumerator
        implements Enumerator<Object[]>
{
    private final Schema schema;
    private final PageConverter valueMapper;
    private final PageReader pageReader;

    public PageEnumerator(Schema schema, PageConverter valueMapper)
    {
        this.schema = schema;
        this.pageReader = new PageReader(schema);
        this.valueMapper = valueMapper;
    }

    public void setPage(Page page)
    {
        this.pageReader.setPage(page);
        this.valueMapper.setPageReader(pageReader);
    }

    @Override
    public Object[] current()
    {
        // this is called from org.apache.calcite.linq4j.EnumerableDefaults
        schema.visitColumns(valueMapper);
        return valueMapper.getRow();
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
