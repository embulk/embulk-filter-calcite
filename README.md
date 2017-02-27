# Apache Calcite filter plugin for Embulk

TODO: Write short description here and build.gradle file.

## Overview

* **Plugin type**: filter

## Configuration

- **query**: description (string, required)

## Example

```yaml
filters:
  - type: calcite
    query: *
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
