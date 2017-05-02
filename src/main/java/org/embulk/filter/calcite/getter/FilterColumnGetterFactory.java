package org.embulk.filter.calcite.getter;

import org.embulk.input.jdbc.AbstractJdbcInputPlugin;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcInputConnection;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;

public class FilterColumnGetterFactory
    extends ColumnGetterFactory {

  private final DateTimeZone defaultTimeZone;

  /**
   * Creates a factory object to create {@code ColumnGetter}s for converting JdbcType to Embulk
   * type.
   *
   * @param to a {@code PageBuilder} object that is passed to column getters.
   * @param defaultTimeZone a {@code DateTimeZone} object passed to timestamp column getters as
   *                        default.
   */
  public FilterColumnGetterFactory(PageBuilder to, DateTimeZone defaultTimeZone) {
    super(to, defaultTimeZone);
    // TODO make change super.defaultTimeZone field protected
    this.defaultTimeZone = defaultTimeZone;
  }

  @Override
  public ColumnGetter newColumnGetter(JdbcInputConnection con,
      AbstractJdbcInputPlugin.PluginTask task, JdbcColumn column, JdbcColumnOption option) {
    String valueType = option.getValueType();
    Type toType = getToType(option);
    if (valueType.equals("coalesce") && sqlTypeToValueType(column, column.getSqlType())
        .equals("timestamp")) {
      return new FilterTimestampColumnGetter(to, toType, option.getTimeZone().or(defaultTimeZone));
    } else {
      return super.newColumnGetter(con, task, column, option);
    }
  }
}
