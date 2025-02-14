# Laketower

> Oversee your lakehouse

## Usage

### Configuration

```yaml
tables:
  - name: sample_table
    uri: demo/sample_table
    format: delta
  - name: weather
    uri: demo/weather
    format: delta
```

### CLI

```bash
$ laketower --help
usage: laketower [-h] [--config CONFIG] {config,tables} ...

options:
  -h, --help           show this help message and exit
  --config, -c CONFIG  Path to the Laketower YAML configuration file

commands:
  {config,tables}
    config             Work with configuration
    tables             Work with tables
```

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

#### View a given table schema

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

#### Query all registered tables

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

## License

Licensed under GNU Affero General Public License v3.0 (AGPLv3)

Copyright (c) 2025 - present Romain Clement