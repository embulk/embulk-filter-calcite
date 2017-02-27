Embulk::JavaPlugin.register_filter(
  "calcite", "org.embulk.filter.calcite.CalciteFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
