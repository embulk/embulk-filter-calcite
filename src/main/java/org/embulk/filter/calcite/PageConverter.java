package org.embulk.filter.calcite;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * This class converts Embulk's Page values into Calcite's row types. It refers to
 * org.apache.calcite.adapter.csv.CsvEnumerator.
 */
public class PageConverter
        implements ColumnVisitor
{
    private final TimeZone defaultTimeZone;
    private final Object[] row;
    private PageReader pageReader;

    public PageConverter(Schema schema, TimeZone defaultTimeZone)
    {
        this.defaultTimeZone = defaultTimeZone;
        this.row = new Object[schema.getColumnCount()];
    }

    public Object[] getRow()
    {
        return row;
    }

    public void setPageReader(PageReader pageReader)
    {
        this.pageReader = pageReader;
    }

    @Override
    public void booleanColumn(Column column)
    {
        // Embulk's boolean is converted into Java's boolean
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        }
        else {
            row[i] = pageReader.getBoolean(i);
        }
    }

    @Override
    public void longColumn(Column column)
    {
        // Embulk's long is converted into long type
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        }
        else {
            row[i] = pageReader.getLong(i);
        }
    }

    @Override
    public void doubleColumn(Column column)
    {
        // Embulk's double is converted into java.math.BigDecimal
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        }
        else {
            row[i] = new BigDecimal(pageReader.getDouble(i));
        }
    }

    @Override
    public void stringColumn(Column column)
    {
        // Embulk's string is converted into java.lang.String
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        }
        else {
            row[i] = pageReader.getString(i);
        }
    }

    @Override
    public void timestampColumn(Column column)
    {
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        }
        else {
            // Embulk's timestamp is converted into java.util.Calendar
            org.embulk.spi.time.Timestamp timestamp = pageReader.getTimestamp(i);
            Calendar c = GregorianCalendar.getInstance(defaultTimeZone);
            c.setTimeInMillis(timestamp.getEpochSecond() * 1000 + timestamp.getNano() / 1000000);
            row[i] = c;
        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        // Embulk's json is converted into Java's string
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        }
        else {
            row[i] = pageReader.getJson(i).toJson();
        }
    }
}
