package org.embulk.filter.calcite.getter;

import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;

public class FilterColumnGetterFactory
        extends ColumnGetterFactory
{
    private final DateTimeZone defaultTimeZone;

    public FilterColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone)
    {
        super(to, defaultTimeZone);
        this.defaultTimeZone = defaultTimeZone; // TODO make change super.defaultTimeZone field protected
    }

    @Override
    public ColumnGetter newColumnGetter(JdbcInputConnection con, AbstractJdbcInputPlugin.PluginTask task, JdbcColumn column, JdbcColumnOption option)
    {
        String valueType = option.getValueType();
        Type toType = getToType(option);
        if (valueType.equals("coalesce") && sqlTypeToValueType(column, column.getSqlType()).equals("timestamp")) {
            return new UTCTimestampColumnGetter(to, toType, newTimestampFormatter(option, "%Y-%m-%d"), option.getTimeZone().or(defaultTimeZone));
        }
        else {
            return super.newColumnGetter(con, task, column, option);
        }
    }

    private TimestampFormatter newTimestampFormatter(JdbcColumnOption option, String defaultTimestampFormat)
    {
        return new TimestampFormatter(
                option.getJRuby(),
                option.getTimestampFormat().isPresent() ? option.getTimestampFormat().get().getFormat() : defaultTimestampFormat,
                option.getTimeZone().or(defaultTimeZone));
    }
}
