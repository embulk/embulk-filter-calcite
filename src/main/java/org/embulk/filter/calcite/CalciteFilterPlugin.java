package org.embulk.filter.calcite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.calcite.jdbc.Driver;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
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
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.ZoneIdModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalciteFilterPlugin implements FilterPlugin {
    protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
            ConfigMapperFactory.builder().addDefaultModules().addModule(ZoneIdModule.withLegacyNames()).build();

    protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    protected static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    private static final Logger log = LoggerFactory.getLogger(CalciteFilterPlugin.class);

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

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
                throw new RuntimeException(e);
            }

            control.run(task.dump(), buildOutputSchema(task, querySchema));
        } finally {
            PageTable.pageConverter.remove();
        }
    }

    private void setupPropertiesFromTransaction(PluginTask task, Properties props) {
        setupProperties(task, props);
    }

    private void setupProperties(PluginTask task, Properties props) {
        // @see https://calcite.apache.org/docs/adapter.html#jdbc-connect-string-parameters
        final Map<String, String> options = task.getOptions();
        props.setProperty("timeZone", task.getDefaultTimeZone().getId());

        // overwrites props with 'options' option
        props.putAll(options);
    }

    private PageConverter newPageConverter(PluginTask task, Schema inputSchema) {
        return new PageConverter(inputSchema, TimeZone.getTimeZone(task.getDefaultTimeZone()));
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
            throw new RuntimeException(e);
        }
    }

    private String buildJdbcUrl() {
        // build a json model to apply Page storage adaptor
        // @see https://github.com/apache/calcite/blob/master/example/csv/src/test/resources/model.json

        final HashMap<String, Object> map = new HashMap<>();
        map.put("version", "1.0");
        map.put("defaultSchema", "page");

        final ArrayList<Map<String, String>> schemas = new ArrayList<>();
        final HashMap<String, String> schema = new HashMap<>();
        schema.put("name", "page");
        schema.put("type", "custom");
        schema.put("factory", PageSchemaFactory.class.getName());
        schemas.add(schema);

        map.put("schemas", schemas);

        final String jsonModel;
        try {
            jsonModel = (new ObjectMapper()).writeValueAsString(map);
        } catch (final JsonProcessingException ex) {
            throw new RuntimeException("Unexpected fatal error.", ex);
        }

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
        final ArrayList<JdbcColumn> columns = new ArrayList<>();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            int index = i + 1; // JDBC column index begins from 1
            columns.add(new JdbcColumn(
                    metadata.getColumnLabel(index),
                    metadata.getColumnTypeName(index),
                    metadata.getColumnType(index),
                    metadata.getPrecision(index),
                    metadata.getScale(index)));
        }
        return new JdbcSchema(Collections.unmodifiableList(columns));
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
        ColumnGetterFactory factory = newColumnGetterFactory(task, Optional.<PageBuilder>empty());
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
        final ArrayList<ColumnGetter> getters = new ArrayList<>();
        for (JdbcColumn column : querySchema.getColumns()) {
            getters.add(factory.newColumnGetter(null, null, column, newJdbcColumnOption()));
        }
        return Collections.unmodifiableList(getters);
    }

    private JdbcColumnOption newJdbcColumnOption() {
        // TODO need to improve for supporting column_options: option
        return CONFIG_MAPPER.map(CONFIG_MAPPER_FACTORY.newConfigSource(), JdbcColumnOption.class);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema, Schema outputSchema,
                           PageOutput output) {
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        // Set input schema in PageSchema for various types of executor plugins
        PageSchema.schema = inputSchema;

        final PageBuilder pageBuilder = getPageBuilder(Exec.getBufferAllocator(), outputSchema, output);
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
        public ZoneId getDefaultTimeZone();

        public JdbcSchema getQuerySchema();

        public void setQuerySchema(JdbcSchema querySchema);

        // TODO support jdbc Url properties
        // TODO support column_options: option

        @Config("options")
        @ConfigDefault("{}")
        public Map<String, String> getOptions();
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
                throw new RuntimeException(e); // TODO better exception handling? error messages?
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
                throw new RuntimeException(e);
            }
        }
    }

    @SuppressWarnings("deprecation")
    private static PageBuilder getPageBuilder(final BufferAllocator bufferAllocator, final Schema schema, final PageOutput output) {
        if (HAS_EXEC_GET_PAGE_BUILDER) {
            return Exec.getPageBuilder(bufferAllocator, schema, output);
        } else {
            return new PageBuilder(bufferAllocator, schema, output);
        }
    }

    private static boolean hasExecGetPageBuilder() {
        try {
            Exec.class.getMethod("getPageBuilder", BufferAllocator.class, Schema.class, PageOutput.class);
        } catch (final NoSuchMethodException ex) {
            return false;
        }
        return true;
    }

    private static final boolean HAS_EXEC_GET_PAGE_BUILDER = hasExecGetPageBuilder();
}
