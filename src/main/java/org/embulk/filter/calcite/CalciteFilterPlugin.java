package org.embulk.filter.calcite;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.calcite.jdbc.Driver;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.filter.calcite.adapter.page.PageSchema;
import org.embulk.filter.calcite.adapter.page.PageSchemaFactory;
import org.embulk.filter.calcite.adapter.page.PageTable;
import org.embulk.filter.calcite.getter.FilterColumnGetterFactory;
import org.embulk.input.jdbc.JdbcColumn;
import org.embulk.input.jdbc.JdbcColumnOption;
import org.embulk.input.jdbc.JdbcSchema;
import org.embulk.input.jdbc.getter.ColumnGetter;
import org.embulk.input.jdbc.getter.ColumnGetterFactory;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Locale.ENGLISH;
import static org.embulk.spi.Exec.getLogger;
import static org.embulk.spi.Exec.getModelManager;
import static org.embulk.spi.Exec.newConfigSource;

public class CalciteFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("query")
        public String getQuery();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public DateTimeZone getDefaultTimeZone();

        public JdbcSchema getQuerySchema();
        public void setQuerySchema(JdbcSchema querySchema);

        // TODO support jdbc Url properties
        // TODO support column_options: option

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    private final Logger log;

    @Inject
    public CalciteFilterPlugin()
    {
        this.log = getLogger(getClass());
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Properties props = System.getProperties(); // TODO should be configured as config option
        PageSchema.schema.set(inputSchema); // Set input schema as Page table schema

        JdbcSchema querySchema;
        try (Connection conn = newConnection(props)) { // SQLException thrown by conn.close()
            querySchema = getQuerySchema(task, conn);
            task.setQuerySchema(querySchema);
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }

        control.run(task.dump(), buildOutputSchema(task, querySchema));
    }

    private Connection newConnection(Properties props)
    {
        String jdbcUrl = buildJdbcUrl();
        try {
            // Relax case-sensitive
            // @see https://calcite.apache.org/docs/adapter.html#jdbc-connect-string-parameters
            props.setProperty("caseSensitive", "false");
            return new Driver().connect(jdbcUrl, props);
        }
        catch (SQLException e) {
            String message = format(ENGLISH, "Cannot create connections by Jdbc URL: %s", jdbcUrl);
            throw new IllegalStateException(message, e);
        }
    }

    private String buildJdbcUrl()
    {
        // build custom model
        ImmutableMap.Builder<String, Object> map = ImmutableMap.builder();
        map.put("version", "1.0");
        map.put("defaultSchema", "page");
        map.put("schemas", ImmutableList.<Map<String, String>>of(
                ImmutableMap.of(
                        "name", "page",
                        "type", "custom",
                        "factory", PageSchemaFactory.class.getName()
                )
        ));
        String customModel = getModelManager().writeObject(map.build());

        // build Jdbc URL
        String jdbcUrl = format(ENGLISH, "jdbc:calcite:model=inline:%s", customModel);
        log.info(format(ENGLISH, "Generated Jdbc URL: %s", jdbcUrl));
        return jdbcUrl;
    }

    private JdbcSchema getQuerySchema(PluginTask task, Connection conn)
            throws SQLException
    {
        try (Statement stat = conn.createStatement(); // SQLException thrown by conn.close()
                ResultSet result = executeQuery(stat, task.getQuery())) { // SQLException thrown by rs.close()
            return getQuerySchema(result.getMetaData());
        }
    }

    private ResultSet executeQuery(Statement stat, String query)
    {
        // This is a workaround to avoid NPE caused by commons-compiler v2.7.6
        ClassLoader cl = currentThread().getContextClassLoader();
        currentThread().setContextClassLoader(getClass().getClassLoader());
        try {
            return stat.executeQuery(query);
        }
        catch (SQLException e) {
            throw new ConfigException("Cannot execute a query: " + query, e);
        }
        finally {
            currentThread().setContextClassLoader(cl);
        }
    }

    private JdbcSchema getQuerySchema(ResultSetMetaData metadata)
            throws SQLException
    {
        ImmutableList.Builder<JdbcColumn> columns = ImmutableList.builder();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            int index = i + 1; // JDBC column index begins from 1
            columns.add(new JdbcColumn(
                    metadata.getColumnLabel(index),
                    metadata.getColumnTypeName(index),
                    metadata.getColumnType(index),
                    metadata.getPrecision(index),
                    metadata.getScale(index)));
        }
        return new JdbcSchema(columns.build());
    }

    private Schema buildOutputSchema(PluginTask task, JdbcSchema querySchema)
    {
        ColumnGetterFactory factory = newColumnGetterFactory(task, null);
        Schema.Builder schema = Schema.builder();
        for (JdbcColumn column : querySchema.getColumns()) {
            String name = column.getName();
            Type type = factory.newColumnGetter(null, null, column, newJdbcColumnOption()).getToType();
            schema.add(name, type);
        }
        return schema.build();
    }

    private ColumnGetterFactory newColumnGetterFactory(PluginTask task, PageBuilder pageBuilder)
    {
        return new FilterColumnGetterFactory(pageBuilder, task.getDefaultTimeZone());
    }

    private List<ColumnGetter> newColumnGetters(ColumnGetterFactory factory, JdbcSchema querySchema)
    {
        ImmutableList.Builder<ColumnGetter> getters = ImmutableList.builder();
        for (JdbcColumn column : querySchema.getColumns()) {
            JdbcColumnOption columnOption = newJdbcColumnOption();
            getters.add(factory.newColumnGetter(null, null, column, columnOption));
        }
        return getters.build();
    }

    private JdbcColumnOption newJdbcColumnOption()
    {
        // TODO need to improve for supporting column_options: option
        return newConfigSource().loadConfig(JdbcColumnOption.class);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        Properties props = System.getProperties(); // TODO should be configured as config option
        PageSchema.schema.set(inputSchema); // Set input schema as Page table schema
        PageBuilder pageBuilder = new PageBuilder(task.getBufferAllocator(), outputSchema, output);
        ColumnGetterFactory factory = newColumnGetterFactory(task, pageBuilder);
        List<ColumnGetter> getters = newColumnGetters(factory, task.getQuerySchema());
        return new FilterPageOutput(outputSchema, task.getQuery(), pageBuilder, getters, props);
    }

    class FilterPageOutput
            implements PageOutput
    {
        private final Schema outputSchema;
        private final String query;
        private final PageBuilder pageBuilder;
        private final List<ColumnGetter> getters;
        private final Properties props;

        FilterPageOutput(Schema outputSchema, String query, PageBuilder pageBuilder,
                List<ColumnGetter> getters, Properties props)
        {
            this.outputSchema = outputSchema;
            this.query = query;
            this.pageBuilder = pageBuilder;
            this.getters = getters;
            this.props = props;
        }

        @Override
        public void add(Page page)
        {
            PageTable.page.set(page);
            try (Connection conn = newConnection(props);
                    Statement stat = conn.createStatement();
                    ResultSet result = executeQuery(stat, query)) {

                while (result.next()) {
                    for (int i = 0; i < getters.size(); i++) {
                        int index = i + 1; // JDBC column index begins from 1
                        getters.get(i).getAndSet(result, index, outputSchema.getColumn(i));
                    }
                    pageBuilder.addRecord();
                }
            }
            catch (SQLException e) {
                throw Throwables.propagate(e); // TODO better exception handling? error messages?
            }
            finally {
                PageTable.page.remove();
            }
        }

        @Override
        public void finish()
        {
            pageBuilder.finish();
        }

        @Override
        public void close()
        {
            pageBuilder.close();
        }
    }
}