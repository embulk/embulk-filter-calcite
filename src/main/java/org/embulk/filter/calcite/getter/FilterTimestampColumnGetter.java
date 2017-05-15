package org.embulk.filter.calcite.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.TimeZone;

import org.embulk.input.jdbc.getter.TimestampColumnGetter;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;

public class FilterTimestampColumnGetter
        extends TimestampColumnGetter {

    private static final ThreadLocal<Calendar> calendar = new ThreadLocal<>();

    public FilterTimestampColumnGetter(PageBuilder to, Type toType, DateTimeZone timeZone) {
        super(to, toType, null);
        calendar.set(Calendar.getInstance(TimeZone.getTimeZone(timeZone.getID()))); // set TLS here
    }

    @Override
    protected void fetch(ResultSet from, int fromIndex)
            throws SQLException {
        java.sql.Timestamp timestamp = from.getTimestamp(fromIndex, calendar.get());
        if (timestamp != null) {
            value = Timestamp.ofEpochSecond(timestamp.getTime() / 1000, timestamp.getNanos());
        }
    }
}
