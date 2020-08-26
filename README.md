# Apache Calcite filter plugin for Embulk

[![Build Status](https://github.com/embulk/embulk-filter-calcite/workflows/Build%20and%20test/badge.svg)](https://github.com/embulk/embulk-filter-calcite/actions?query=workflow%3A%22Build+and+test%22)

## Overview

* **Plugin type**: filter

This plugin allows users to translate rows flexibly by SQL queries specified by them.

## Architecture

This plugin allows translating rows by SQL queries in Pages received from input plugin and sending the query results to next filter or output plugin as modified Pages. It uses [Apache Calcite](https://calcite.apache.org/), which is the foundation for your next high-performance database and enbles executing SQL queries to customized storage by the [custom adaptor](https://calcite.apache.org/docs/tutorial.html). The plugin applies Page storage adaptor to Apache Calcite and then enables executing SQL queries to Pages via JDBC Driver provided.

Here is Embulk config example for this plugin:

```yaml
filters:
  - type: calcite
    query: SELECT * FROM $PAGES
```

Users can define `SELECT` query as query option in the filter config section. `$PAGES` represents Pages that input plugin creates and sends. `$PAGES` schema is Embulk input schema given. On the other hand, the output schema of the plugin is built from the metadata of query result. Embulk types are converted into Apache Calcite types internally. This is type mapping between Embulk and Apache Calcite.

| Embulk type | Apache Calcite type |      JDBC type      |
| ----------- | ------------------- | ------------------- |
| boolean     | BOOLEAN             | java.lang.Boolean   |
| long        | BIGINT              | java.lang.Long      |
| double      | DOUBLE              | java.lang.Double    |
| timestamp   | TIMESTAMP           | java.sql.Timestamp  |
| string      | VARCHAR             | java.lang.String    |
| json        | VARCHAR             | java.lang.String    |

Data types by Apache Calcite: https://calcite.apache.org/docs/reference.html#data-types

## Configuration

- **query**: SQL to run (string, required)
- **default_timezone**: Configure timezone that is used for JDBC connection properties and Calcite engine. This option is one of [JDBC connect parameters](https://calcite.apache.org/docs/adapter.html#jdbc-connect-string-parameters) provided by Apache Calcite. java.util.TimeZone's [AvailableIDs](http://docs.oracle.com/javase/7/docs/api/java/util/TimeZone.html#getAvailableIDs) can be specified. (string, default: 'UTC')
- **options**: extra JDBC properties. See [JDBC connect parameters](https://calcite.apache.org/docs/adapter.html#jdbc-connect-string-parameters). (hash, default: {})


## Example

This config enables removing rows not associated to id 1 and 2 from Pages.
```yaml
filters:
  - type: calcite
    query: SELECT * FROM $PAGES WHERE id IN (1, 2)
    options:
      caseSensitive: false # this option require when lowser column name use.
```

The following is an example by LIKE operator and enables removing rows not matched at a specified pattern from Pages.
```yaml
filters:
  - type: calcite
    query: SELECT * FROM $PAGES WHERE message LIKE '%EMBULK%'
    options:
      caseSensitive: false # this option require when lowser column name use.
```

This enables adding new column and inserting the value combined 2 string column values.
```yaml
filters:
  - type: calcite
    query: SELECT first_name || last_name AS name, * FROM $PAGES
    options:
      caseSensitive: false # this option require when lowser column name use.
```

Adds the new column by CURRENT_TIMESTAMP function.
```yaml
filters:
  - type: calcite
    query: SELECT CURRENT_TIMESTAMP, * FROM $PAGES
    default_timezone: 'America/Los_Angeles'
```

SQL language provided by Apache Calcite: https://calcite.apache.org/docs/reference.html

## Build

```
$ ./gradlew gem
```

## Release

```
$ ./gradlew gemPush
```
