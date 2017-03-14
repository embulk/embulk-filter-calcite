package org.embulk.filter.calcite.getter;

import org.embulk.input.jdbc.getter.TimestampColumnGetter;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Type;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

import static java.util.Calendar.getInstance;
import static java.util.TimeZone.getTimeZone;

public class UTCTimestampColumnGetter
        extends TimestampColumnGetter
{
    private static ThreadLocal<Calendar> calendar = new ThreadLocal<Calendar>() {
        @Override
        protected Calendar initialValue()
        {
            return getInstance(getTimeZone("UTC"));
        }
    };

    public UTCTimestampColumnGetter(PageBuilder to, Type toType, TimestampFormatter timestampFormatter)
    {
        super(to, toType, timestampFormatter);
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex)
            throws SQLException
    {
        java.sql.Timestamp timestamp = from.getTimestamp(fromIndex, calendar.get());
        if (timestamp != null) {
            value = Timestamp.ofEpochSecond(timestamp.getTime() / 1000, timestamp.getNanos());
        }
    }
}
