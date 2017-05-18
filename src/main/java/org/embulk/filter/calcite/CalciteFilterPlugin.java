package org.embulk.filter.calcite;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
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
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.unit.ToStringMap;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;

public class CalciteFilterPlugin implements FilterPlugin {

    private final Logger log;

    @Inject
    public CalciteFilterPlugin() {
        this.log = Exec.getLogger(getClass());
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);
        Properties props = System.getProperties(); // TODO should be configured as config option
        setupPropertiesFromTransaction(task, props);

        // Set input schema in PageSchema
        PageSchema.schema = inputSchema;

        // Set page converter as TLS variable in PageTable
        PageTable.pageConverter.set(newPageConverter(task, inputSchema));

        final String jdbcUrl = buildJdbcUrl();
        log.info(String.format(Locale.ENGLISH, "Generated Jdbc URL: %s", jdbcUrl));

        try {
            JdbcSchema querySchema;
            try (Connection conn = newConnection(jdbcUrl, props)) { // SQLException by conn.close()
                querySchema = getQuerySchema(task, conn);
                task.setQuerySchema(querySchema);
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }

            control.run(task.dump(), buildOutputSchema(task, querySchema));
        } finally {
            PageTable.pageConverter.remove();
        }
    }

    private void setupPropertiesFromTransaction(PluginTask task, Properties props) {
        final ToStringMap options = task.getOptions();
        if (!options.containsKey("caseSensitive")) {
            log.warn("JDBC parameter 'caseSensitive' is implicitly set to false as default in");
            log.warn("embulk-filter-calcite 0.1 but, it's scheduled to change default with true");
            log.warn("in 0.2. Please use 'options' option to set 'caseSensitive' to false.");
        }
        setupProperties(task, props);
    }

    private void setupProperties(PluginTask task, Properties props) {
        // @see https://calcite.apache.org/docs/adapter.html#jdbc-connect-string-parameters
        final ToStringMap options = task.getOptions();
        props.setProperty("caseSensitive", "false"); // Relax case-sensitive
        props.setProperty("timeZone", task.getDefaultTimeZone().getID());

        // overwrites props with 'options' option
        props.putAll(options);
    }

    private PageConverter newPageConverter(PluginTask task, Schema inputSchema) {
        return new PageConverter(inputSchema, task.getDefaultTimeZone().toTimeZone());
    }

    private Connection newConnection(String jdbcUrl, Properties props) {
        try {
            return new Driver().connect(jdbcUrl, props);
        } catch (SQLException e) {
            String message = String.format(Locale.ENGLISH,
                    "Cannot create connections by Jdbc URL: %s", jdbcUrl);
            throw new IllegalStateException(message, e);
        }
    }

    private PreparedStatement createPreparedStatement(Connection conn, String query) {
        try {
            return conn.prepareStatement(query);
        } catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private String buildJdbcUrl() {
        // build a json model to apply Page storage adaptor
        // @see https://github.com/apache/calcite/blob/master/example/csv/src/test/resources/model.json
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
        String jsonModel = Exec.getModelManager().writeObject(map.build());

        // build Jdbc URL
        return String.format(Locale.ENGLISH, "jdbc:calcite:model=inline:%s", jsonModel);
    }

    private JdbcSchema getQuerySchema(PluginTask task, Connection conn)
            throws SQLException {
        try (Statement stat = conn.createStatement(); // SQLException thrown by conn.close()
             ResultSet result = executeQuery(stat,
                     task.getQuery())) { // SQLException thrown by rs.close()
            return getQuerySchema(result.getMetaData());
        }
    }

    private JdbcSchema getQuerySchema(ResultSetMetaData metadata)
            throws SQLException {
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

    private ResultSet executeQuery(Statement stat, String query) {
        // This is a workaround to avoid NPE caused by commons-compiler v2.7.6
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        try {
            return stat.executeQuery(query);
        } catch (SQLException e) {
            throw new ConfigException("Cannot execute a query: " + query, e);
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    private Schema buildOutputSchema(PluginTask task, JdbcSchema querySchema) {
        ColumnGetterFactory factory = newColumnGetterFactory(task, Optional.<PageBuilder>absent());
        List<ColumnGetter> getters = newColumnGetters(factory, querySchema);

        Schema.Builder schema = Schema.builder();
        for (int i = 0; i < querySchema.getColumns().size(); i++) {
            schema.add(querySchema.getColumn(i).getName(), getters.get(i).getToType());
        }
        return schema.build();
    }

    private ColumnGetterFactory newColumnGetterFactory(PluginTask task,
                                                       Optional<PageBuilder> pageBuilder) {
        if (pageBuilder.isPresent()) {
            return new FilterColumnGetterFactory(pageBuilder.get(), task.getDefaultTimeZone());
        } else {
            return new FilterColumnGetterFactory(null, task.getDefaultTimeZone());
        }
    }

    private List<ColumnGetter> newColumnGetters(ColumnGetterFactory factory,
                                                JdbcSchema querySchema) {
        ImmutableList.Builder<ColumnGetter> getters = ImmutableList.builder();
        for (JdbcColumn column : querySchema.getColumns()) {
            getters.add(factory.newColumnGetter(null, null, column, newJdbcColumnOption()));
        }
        return getters.build();
    }

    private JdbcColumnOption newJdbcColumnOption() {
        // TODO need to improve for supporting column_options: option
        return Exec.newConfigSource().loadConfig(JdbcColumnOption.class);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema, Schema outputSchema,
                           PageOutput output) {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // Set input schema in PageSchema for various types of executor plugins
        PageSchema.schema = inputSchema;

        PageBuilder pageBuilder = new PageBuilder(task.getBufferAllocator(), outputSchema, output);
        PageConverter pageConverter = newPageConverter(task, inputSchema);
        ColumnGetterFactory factory = newColumnGetterFactory(task, Optional.of(pageBuilder));
        List<ColumnGetter> getters = newColumnGetters(factory, task.getQuerySchema());
        Properties props = System.getProperties(); // TODO should be configured as config option
        setupProperties(task, props);
        final Connection conn = newConnection(buildJdbcUrl(), props);
        final PreparedStatement preparedStatement = createPreparedStatement(conn, task.getQuery());
        return new FilterPageOutput(outputSchema,
                pageBuilder,
                pageConverter,
                getters,
                preparedStatement); // Transfer ownership of preparedStatement to FilterPageOutput
    }

    public interface PluginTask
            extends Task {

        @Config("query")
        public String getQuery();

        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public DateTimeZone getDefaultTimeZone();

        public JdbcSchema getQuerySchema();

        public void setQuerySchema(JdbcSchema querySchema);

        // TODO support jdbc Url properties
        // TODO support column_options: option

        @Config("options")
        @ConfigDefault("{}")
        public ToStringMap getOptions();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    private class FilterPageOutput
            implements PageOutput {

        private final Schema outputSchema;
        private final PageBuilder pageBuilder;
        private final PageConverter pageConverter;
        private final List<ColumnGetter> getters;
        private final PreparedStatement preparedStatement;

        private FilterPageOutput(Schema outputSchema,
                                 PageBuilder pageBuilder,
                                 PageConverter pageConverter,
                                 List<ColumnGetter> getters,
                                 PreparedStatement preparedStatement) {
            this.outputSchema = outputSchema;
            this.pageBuilder = pageBuilder;
            this.pageConverter = pageConverter;
            this.getters = getters;
            this.preparedStatement = preparedStatement;
        }

        @Override
        public void add(Page page) {
            // Set page converter as TLS variable in PageTable
            PageTable.pageConverter.set(pageConverter);

            // Set page as TLS variable in PageTable
            PageTable.page.set(page);


            try (ResultSet result = preparedStatement.executeQuery()) {
                while (result.next()) {
                    for (int i = 0; i < getters.size(); i++) {
                        int index = i + 1; // JDBC column index begins from 1
                        getters.get(i).getAndSet(result, index, outputSchema.getColumn(i));
                    }
                    pageBuilder.addRecord();
                }
            } catch (SQLException e) {
                throw Throwables.propagate(e); // TODO better exception handling? error messages?
            } finally {
                PageTable.pageConverter.remove();
                PageTable.page.remove();
            }
        }

        @Override
        public void finish() {
            pageBuilder.finish();
        }

        @Override
        public void close() {
            pageBuilder.close();
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
