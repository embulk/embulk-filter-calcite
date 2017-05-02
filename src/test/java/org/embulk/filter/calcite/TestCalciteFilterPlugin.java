package org.embulk.filter.calcite;

import static org.embulk.test.EmbulkTests.copyResource;
import static org.embulk.test.EmbulkTests.readResource;
import static org.embulk.test.EmbulkTests.readSortedFile;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Path;
import org.embulk.config.ConfigSource;
import org.embulk.spi.FilterPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestCalciteFilterPlugin {

  private static final String RESOURCE_NAME_PREFIX = "org/embulk/filter/calcite/test/";

  @Rule
  public TestingEmbulk embulk = TestingEmbulk.builder()
      .registerPlugin(FilterPlugin.class, "calcite", CalciteFilterPlugin.class)
      .build();

  private ConfigSource baseConfig;

  static void assertRecordsByResource(TestingEmbulk embulk,
      String inConfigYamlResourceName, String filterConfigYamlResourceName,
      String sourceCsvResourceName, String resultCsvResourceName)
      throws IOException {
    Path inputPath = embulk.createTempFile("csv");
    Path outputPath = embulk.createTempFile("csv");

    // in: config
    copyResource(RESOURCE_NAME_PREFIX + sourceCsvResourceName, inputPath);
    ConfigSource inConfig = embulk.loadYamlResource(RESOURCE_NAME_PREFIX + inConfigYamlResourceName)
        .set("path_prefix", inputPath.toAbsolutePath().toString());

    // remove_columns filter config
    ConfigSource filterConfig = embulk
        .loadYamlResource(RESOURCE_NAME_PREFIX + filterConfigYamlResourceName);

    TestingEmbulk.RunResult result = embulk.inputBuilder()
        .in(inConfig)
        .filters(ImmutableList.of(filterConfig))
        .outputPath(outputPath)
        .run();

    assertThat(readSortedFile(outputPath),
        is(readResource(RESOURCE_NAME_PREFIX + resultCsvResourceName)));
  }

  @Before
  public void setup() {
    baseConfig = embulk.newConfig();
  }

  @Test
  public void testSimple() throws Exception {
    assertRecordsByResource(embulk, "test_simple_in.yml", "test_simple_filter.yml",
        "test_simple_source.csv", "test_simple_expected.csv");
  }

  @Test
  public void testIntOperators() throws Exception {
    assertRecordsByResource(embulk, "test_int_ops_in.yml", "test_int_ops_filter.yml",
        "test_int_ops_source.csv", "test_int_ops_expected.csv");
  }

  @Test
  public void testWhereIntCondition() throws Exception {
    assertRecordsByResource(embulk, "test_where_int_cond_in.yml", "test_where_int_cond_filter.yml",
        "test_where_int_cond_source.csv", "test_where_int_cond_expected.csv");
  }

  @Test
  public void testStringOperators() throws Exception {
    assertRecordsByResource(embulk, "test_string_ops_in.yml", "test_string_ops_filter.yml",
        "test_string_ops_source.csv", "test_string_ops_expected.csv");
  }

  @Test
  public void testWhereStringCondition() throws Exception {
    assertRecordsByResource(embulk, "test_where_string_cond_in.yml",
        "test_where_string_cond_filter.yml",
        "test_where_string_cond_source.csv", "test_where_string_cond_expected.csv");
  }
}
