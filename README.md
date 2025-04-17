# Laketower

> Oversee your lakehouse

[![PyPI](https://img.shields.io/pypi/v/laketower.svg)](https://pypi.org/project/laketower/)
[![Python Versions](https://img.shields.io/pypi/pyversions/laketower?logo=python&logoColor=white)](https://pypi.org/project/laketower/)
[![CI/CD](https://github.com/datalpia/laketower/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/datalpia/laketower/actions/workflows/ci-cd.yml)
[![License](https://img.shields.io/github/license/datalpia/laketower)](https://github.com/datalpia/laketower/blob/main/LICENSE)

Utility application to explore and manage tables in your data lakehouse, especially tailored for data pipelines local development.

## Features

- Delta Lake table format support
- Inspect table metadata
- Inspect table schema
- Inspect table history
- Get table statistics
- View table content with a simple query builder
- Query all registered tables with DuckDB SQL dialect
- Execute saved queries
- Static and versionable YAML configuration
- Web application
- CLI application

## Installation

Using `pip` (or any other Python package manager):

```bash
pip install laketower
```

Using `uvx`:

```bash
uvx laketower
```

## Usage

### Configuration

Laketower configuration is based on a static YAML configuration file allowing to:

- List all tables to be registered

Format:

```yaml
tables:
  - name: <table_name>
    uri: <local path to table>
    format: {delta}

queries:
  - name: <query_name>
    title: <Query name>
    sql: <sql expression>
```

Current limitations:

- `tables.uri`: only local paths are allowed
- `tables.format`: only `delta` is allowed

Example from the provided demo:

```yaml
tables:
  - name: sample_table
    uri: demo/sample_table
    format: delta
  - name: weather
    uri: demo/weather
    format: delta

queries:
  - name: all_data
    title: All data
    sql: |
      select
        sample_table.*,
        weather.*
      from
        sample_table,
        weather
      limit 10
  - name: daily_avg_temperature
    title: Daily average temperature
    sql: |
      select
        date_trunc('day', time) as day,
        round(avg(temperature_2m)) as avg_temperature
      from
        weather
      group by
        day
      order by
        day asc
```

### Web Application

The easiest way to get started is to launch the Laketower web application:

```bash
$ laketower -c demo/laketower.yml web
```

#### Screenshots

![Laketower UI - Tables Overview](https://raw.githubusercontent.com/datalpia/laketower/refs/heads/main/docs/static/tables_overview.png)
![Laketower UI - Tables View](https://raw.githubusercontent.com/datalpia/laketower/refs/heads/main/docs/static/tables_view.png)
![Laketower UI - Tables Statistics](https://raw.githubusercontent.com/datalpia/laketower/refs/heads/main/docs/static/tables_statistics.png)
![Laketower UI - Tables History](https://raw.githubusercontent.com/datalpia/laketower/refs/heads/main/docs/static/tables_history.png)
![Laketower UI - Tables Query](https://raw.githubusercontent.com/datalpia/laketower/refs/heads/main/docs/static/tables_query.png)
![Laketower UI - Queries View](https://raw.githubusercontent.com/datalpia/laketower/refs/heads/main/docs/static/queries_view.png)

### CLI

Laketower provides a CLI interface:

```bash
$ laketower --help

usage: laketower [-h] [--version] [--config CONFIG] {web,config,tables,queries} ...

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --config, -c CONFIG   Path to the Laketower YAML configuration file (default: laketower.yml)

commands:
  {web,config,tables,queries}
    web                 Launch the web application
    config              Work with configuration
    tables              Work with tables
    queries             Work with queries
```

By default, a YAML configuration file named `laketower.yml` will be looked for.
A custom path can be specified with the `-c` / `--config` argument.

#### Validate YAML configuration

```bash
$ laketower -c demo/laketower.yml config validate

╭────────────────────────╮
│ Configuration is valid │
╰────────────────────────╯
Config(
    tables=[
        ConfigTable(name='sample_table', uri='demo/sample_table', table_format=<TableFormats.delta: 'delta'>),
        ConfigTable(name='weather', uri='demo/weather', table_format=<TableFormats.delta: 'delta'>)
    ]
)
```

#### List all registered tables

```bash
$ laketower -c demo/laketower.yml tables list

tables
├── sample_table
│   ├── format: delta
│   └── uri: demo/sample_table
└── weather
    ├── format: delta
    └── uri: demo/weather
```

#### Display a given table metadata

```bash
$ laketower -c demo/laketower.yml tables metadata sample_table

sample_table
├── name: Demo table
├── description: A sample demo Delta table
├── format: delta
├── uri: /Users/romain/Documents/dev/datalpia/laketower/demo/sample_table/
├── id: c1cb1cf0-1f3f-47b5-a660-3cc800edd341
├── version: 3
├── created at: 2025-02-05 22:27:39.579000+00:00
├── partitions:
└── configuration: {}
```

#### Display a given table schema

```bash
$ laketower -c demo/laketower.yml tables schema weather

weather
├── time: timestamp[us, tz=UTC]
├── city: string
├── temperature_2m: float
├── relative_humidity_2m: float
└── wind_speed_10m: float
```

#### Display a given table history

```bash
$ uv run laketower -c demo/laketower.yml tables history weather

weather
├── version: 2
│   ├── timestamp: 2025-02-05 22:27:46.425000+00:00
│   ├── client version: delta-rs.0.23.1
│   ├── operation: WRITE
│   ├── operation parameters
│   │   └── mode: Append
│   └── operation metrics
│       ├── execution_time_ms: 4
│       ├── num_added_files: 1
│       ├── num_added_rows: 168
│       ├── num_partitions: 0
│       └── num_removed_files: 0
├── version: 1
│   ├── timestamp: 2025-02-05 22:27:45.666000+00:00
│   ├── client version: delta-rs.0.23.1
│   ├── operation: WRITE
│   ├── operation parameters
│   │   └── mode: Append
│   └── operation metrics
│       ├── execution_time_ms: 4
│       ├── num_added_files: 1
│       ├── num_added_rows: 408
│       ├── num_partitions: 0
│       └── num_removed_files: 0
└── version: 0
    ├── timestamp: 2025-02-05 22:27:39.722000+00:00
    ├── client version: delta-rs.0.23.1
    ├── operation: CREATE TABLE
    ├── operation parameters
    │   ├── metadata: {"configuration":{},"createdTime":1738794459722,"description":"Historical and forecast weather data from
    │   │   open-meteo.com","format":{"options":{},"provider":"parquet"},"id":"a9615fb1-25cc-4546-a0fe-1cb534c514b2","name":"Weather","partitionCol
    │   │   umns":[],"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"time\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},
    │   │   {\"name\":\"city\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"temperature_2m\",\"type\":\"float\",\"nullable\":
    │   │   true,\"metadata\":{}},{\"name\":\"relative_humidity_2m\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}},{\"name\":\"wind_speed_1
    │   │   0m\",\"type\":\"float\",\"nullable\":true,\"metadata\":{}}]}"}
    │   ├── protocol: {"minReaderVersion":1,"minWriterVersion":2}
    │   ├── mode: ErrorIfExists
    │   └── location: file:///Users/romain/Documents/dev/datalpia/laketower/demo/weather
    └── operation metrics
```

#### Get statistics of a given table

Get basic statistics on all columns of a given table:

```bash
$ laketower -c demo/laketower.yml tables statistics weather

┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ column_name          ┃ count ┃ avg                ┃ std                ┃ min                    ┃ max                    ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━┩
│ time                 │ 576   │ None               │ None               │ 2025-01-26 01:00:00+01 │ 2025-02-12 00:00:00+01 │
│ city                 │ 576   │ None               │ None               │ Grenoble               │ Grenoble               │
│ temperature_2m       │ 576   │ 5.2623263956047595 │ 3.326529069892729  │ 0.0                    │ 15.1                   │
│ relative_humidity_2m │ 576   │ 78.76909722222223  │ 15.701802163559918 │ 29.0                   │ 100.0                  │
│ wind_speed_10m       │ 576   │ 7.535763886032833  │ 10.00898058743763  │ 0.0                    │ 42.4                   │
└──────────────────────┴───────┴────────────────────┴────────────────────┴────────────────────────┴────────────────────────┘
```

Specifying a table version yields according results:

```bash
$ laketower -c demo/laketower.yml tables statistics --version 0 weather

┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━┳━━━━━━┳━━━━━━┳━━━━━━┓
┃ column_name          ┃ count ┃ avg  ┃ std  ┃ min  ┃ max  ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━╇━━━━━━╇━━━━━━╇━━━━━━┩
│ time                 │ 0     │ None │ None │ None │ None │
│ city                 │ 0     │ None │ None │ None │ None │
│ temperature_2m       │ 0     │ None │ None │ None │ None │
│ relative_humidity_2m │ 0     │ None │ None │ None │ None │
│ wind_speed_10m       │ 0     │ None │ None │ None │ None │
└──────────────────────┴───────┴──────┴──────┴──────┴──────┘
```

#### View a given table

Using a simple query builder, the content of a table can be displayed.
Optional arguments:

- `--cols <col1> <col2>`: select which columns to display
- `--sort-asc <col>`: sort by a column name in ascending order
- `--sort-desc <col>`: sort by a column name in descending order
- `--limit <num>` (default 10): limit the number of rows
- `--version`: time-travel to table revision number

```bash
$ laketower -c demo/laketower.yml tables view weather

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┓
┃ time                      ┃ city     ┃ temperature_2m     ┃ relative_humidity_2m ┃ wind_speed_10m    ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━┩
│ 2025-02-05 01:00:00+01:00 │ Grenoble │ 2.0                │ 84.0                 │ 4.0               │
│ 2025-02-05 02:00:00+01:00 │ Grenoble │ 2.0999999046325684 │ 83.0                 │ 1.5               │
│ 2025-02-05 03:00:00+01:00 │ Grenoble │ 1.600000023841858  │ 86.0                 │ 1.100000023841858 │
│ 2025-02-05 04:00:00+01:00 │ Grenoble │ 1.899999976158142  │ 80.0                 │ 4.199999809265137 │
│ 2025-02-05 05:00:00+01:00 │ Grenoble │ 1.899999976158142  │ 81.0                 │ 3.299999952316284 │
│ 2025-02-05 06:00:00+01:00 │ Grenoble │ 1.399999976158142  │ 88.0                 │ 4.300000190734863 │
│ 2025-02-05 07:00:00+01:00 │ Grenoble │ 1.7000000476837158 │ 87.0                 │ 5.5               │
│ 2025-02-05 08:00:00+01:00 │ Grenoble │ 1.5                │ 82.0                 │ 4.699999809265137 │
│ 2025-02-05 09:00:00+01:00 │ Grenoble │ 1.899999976158142  │ 80.0                 │ 2.200000047683716 │
│ 2025-02-05 10:00:00+01:00 │ Grenoble │ 2.9000000953674316 │ 80.0                 │ 0.800000011920929 │
└───────────────────────────┴──────────┴────────────────────┴──────────────────────┴───────────────────┘
```

```bash
$ laketower -c demo/laketower.yml tables view weather --cols time city temperature_2m --limit 5 --sort-desc time

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┓
┃ time                      ┃ city     ┃ temperature_2m    ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━┩
│ 2025-02-12 00:00:00+01:00 │ Grenoble │ 5.099999904632568 │
│ 2025-02-12 00:00:00+01:00 │ Grenoble │ 5.099999904632568 │
│ 2025-02-11 23:00:00+01:00 │ Grenoble │ 4.900000095367432 │
│ 2025-02-11 23:00:00+01:00 │ Grenoble │ 4.900000095367432 │
│ 2025-02-11 22:00:00+01:00 │ Grenoble │ 4.900000095367432 │
└───────────────────────────┴──────────┴───────────────────┘
```

```bash
$ laketower -c demo/laketower.yml tables view weather --version 1

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
┃ time                      ┃ city     ┃ temperature_2m    ┃ relative_humidity_2m ┃ wind_speed_10m     ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
│ 2025-01-26 01:00:00+01:00 │ Grenoble │ 7.0               │ 87.0                 │ 8.899999618530273  │
│ 2025-01-26 02:00:00+01:00 │ Grenoble │ 6.099999904632568 │ 87.0                 │ 6.199999809265137  │
│ 2025-01-26 03:00:00+01:00 │ Grenoble │ 6.0               │ 86.0                 │ 2.700000047683716  │
│ 2025-01-26 04:00:00+01:00 │ Grenoble │ 6.099999904632568 │ 82.0                 │ 3.0999999046325684 │
│ 2025-01-26 05:00:00+01:00 │ Grenoble │ 5.5               │ 87.0                 │ 3.299999952316284  │
│ 2025-01-26 06:00:00+01:00 │ Grenoble │ 5.199999809265137 │ 91.0                 │ 2.200000047683716  │
│ 2025-01-26 07:00:00+01:00 │ Grenoble │ 4.800000190734863 │ 86.0                 │ 3.0                │
│ 2025-01-26 08:00:00+01:00 │ Grenoble │ 4.900000095367432 │ 83.0                 │ 1.100000023841858  │
│ 2025-01-26 09:00:00+01:00 │ Grenoble │ 4.0               │ 92.0                 │ 3.0999999046325684 │
│ 2025-01-26 10:00:00+01:00 │ Grenoble │ 5.0               │ 86.0                 │ 6.400000095367432  │
└───────────────────────────┴──────────┴───────────────────┴──────────────────────┴────────────────────┘
```

#### Query all registered tables

Query any registered tables using DuckDB SQL dialect!

```bash
$ laketower -c demo/laketower.yml tables query "select date_trunc('day', time) as day, avg(temperature_2m) as mean_temperature from weather group by day order by day desc limit 3"

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
┃ day                       ┃ mean_temperature   ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
│ 2025-02-12 00:00:00+01:00 │ 5.099999904632568  │
│ 2025-02-11 00:00:00+01:00 │ 4.833333373069763  │
│ 2025-02-10 00:00:00+01:00 │ 2.1083333243926368 │
└───────────────────────────┴────────────────────┘
```

#### List saved queries

```bash
$ laketower -c demo/laketower.yml queries list

queries
├── all_data
└── daily_avg_temperature
```

#### Execute saved queries

```bash
$ laketower -c demo/laketower.yml queries view daily_avg_temperature

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┓
┃ day                       ┃ avg_temperature ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━┩
│ 2025-01-26 00:00:00+01:00 │ 8.0             │
│ 2025-01-27 00:00:00+01:00 │ 13.0            │
│ 2025-01-28 00:00:00+01:00 │ 7.0             │
│ 2025-01-29 00:00:00+01:00 │ 8.0             │
│ 2025-01-30 00:00:00+01:00 │ 9.0             │
│ 2025-01-31 00:00:00+01:00 │ 6.0             │
│ 2025-02-01 00:00:00+01:00 │ 4.0             │
│ 2025-02-02 00:00:00+01:00 │ 4.0             │
│ 2025-02-03 00:00:00+01:00 │ 4.0             │
│ 2025-02-04 00:00:00+01:00 │ 3.0             │
│ 2025-02-05 00:00:00+01:00 │ 3.0             │
│ 2025-02-06 00:00:00+01:00 │ 2.0             │
│ 2025-02-07 00:00:00+01:00 │ 6.0             │
│ 2025-02-08 00:00:00+01:00 │ 7.0             │
│ 2025-02-09 00:00:00+01:00 │ 5.0             │
│ 2025-02-10 00:00:00+01:00 │ 2.0             │
│ 2025-02-11 00:00:00+01:00 │ 5.0             │
│ 2025-02-12 00:00:00+01:00 │ 5.0             │
└───────────────────────────┴─────────────────┘
```

## License

Licensed under [Apache License 2.0](LICENSE)

Copyright (c) 2025 - present Romain Clement
