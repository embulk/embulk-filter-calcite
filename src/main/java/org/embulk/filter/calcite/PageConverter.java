package org.embulk.filter.calcite;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.TimeZone;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

/**
 * This class converts Embulk's Page values into Calcite's row types. It refers to
 * org.apache.calcite.adapter.csv.CsvEnumerator.
 */
public class PageConverter implements ColumnVisitor {

    private final TimeZone defaultTimeZone;
    private final Object[] row;
    private PageReader pageReader;

    public PageConverter(Schema schema, TimeZone defaultTimeZone) {
        this.defaultTimeZone = defaultTimeZone;
        this.row = new Object[schema.getColumnCount()];
    }

    public Object[] getRow() {
        return row;
    }

    public void setPageReader(PageReader pageReader) {
        this.pageReader = pageReader;
    }

    @Override
    public void booleanColumn(Column column) {
        // Embulk's boolean is converted into Java's boolean
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        } else {
            row[i] = pageReader.getBoolean(i);
        }
    }

    @Override
    public void longColumn(Column column) {
        // Embulk's long is converted into long type
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        } else {
            row[i] = pageReader.getLong(i);
        }
    }

    @Override
    public void doubleColumn(Column column) {
        // Embulk's double is converted into java.math.BigDecimal
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        } else {
            row[i] = new BigDecimal(pageReader.getDouble(i));
        }
    }

    @Override
    public void stringColumn(Column column) {
        // Embulk's string is converted into java.lang.String
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        } else {
            row[i] = pageReader.getString(i);
        }
    }

    @Override
    public void timestampColumn(Column column) {
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        } else {
            // Embulk's timestamp is converted into java.sql.Timestmap
            final Instant instant = getInstant(pageReader, i);
            long milliseconds = instant.getEpochSecond() * 1000 + instant.getNano() / 1000000;
            java.sql.Timestamp ts = new java.sql.Timestamp(milliseconds);
            ts.setNanos(instant.getNano());
            row[i] = ts;
        }
    }

    @Override
    public void jsonColumn(Column column) {
        // Embulk's json is converted into Java's string
        int i = column.getIndex();
        if (pageReader.isNull(i)) {
            row[i] = null;
        } else {
            row[i] = pageReader.getJson(i).toJson();
        }
    }

    @SuppressWarnings("deprecation")
    private static Instant getInstant(final PageReader pageReader, final int index) {
        return pageReader.getTimestamp(index).getInstant();
    }
}
