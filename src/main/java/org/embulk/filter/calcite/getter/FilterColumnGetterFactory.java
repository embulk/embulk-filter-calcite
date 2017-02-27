package org.embulk.filter.calcite.getter;

import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.joda.time.DateTimeZone;

public class FilterColumnGetterFactory
        extends ColumnGetterFactory
{
    public FilterColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone)
    {
        super(to, defaultTimeZone);
    }

}
