# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.6] - 2025-11-12
### Fixed
- web: sort query parameters by key name

## [0.6.5] - 2025-11-11
Patch release with minor enhancements (interactive SQL query results, enforced
row limits for large tables, query execution time indicator) and many minor fixes.

### Added
- new `settings` section in YAML configuration
- setting `max_query_rows` to limit sql query results
- setting `web.hide_tables` to hide the tables section for the web application
- query execution time indicator

### Changed
- web: use `datatables.js` for interactive query results

### Fixed
- web: table view hide column button link regression
- web: exclude actions from table data view horizontal scrolling
- web: handle sql query parsing errors
- web: do not highlight multiple menu items sharing the same path prefix

## [0.6.4] - 2025-10-20
### Fixed
- add missing `tzdata` dependency from previous `pandas` dependency removal

## [0.6.3] - 2025-10-19
Patch release removing unnecessary `pandas` dependency and updating the displayed
application name in the web application.

### Fixed
- web: update application name
- web: move application details to about modal window

### Misc
- replace usage of `pandas.DataFrame` with `pyarrow.Table`

## [0.6.2] - 2025-09-28
Patch release fixing a bug when registering Arrow Datasets as tables instead of
views with DuckDB query engine, leading to performance degradation on larger tables.

### Fixed
- map arrow datasets as views with DuckDB query engine

## [0.6.1] - 2025-09-11
Patch release with minor enhancements (SQL syntax highlighting, query parameters,
predefined query Markdown description) and quality of life improvements
(hide read-only SQL editor in predefined queries, web app offline usage).

### Added
- web: display app version in sidebar
- web: use CodeMirror SQL query editor
- web: add support for tables query parameters
- web: add support for predefined queries parameters
- web: add optional markdown description for predefined queries
- cli: add support for tables query parameters
- cli: add support for predefined queries parameters

### Changed
- web: hide SQL editor for predefined queries
- demo: add parameters to daily avg temperature query

### Fixed
- handle empty SQL queries

### Misc
- vendor static assets for offline usage

## [0.6.0] - 2025-08-27
Minor release with new features (CSV import/export, S3/ADLS remote tables)
and quality of life improvements (tables lazy loading, quoted SQL identifiers).

### Added
- cli: add csv export option to tables query command
- cli: add tables import command
- web: add csv export to query views
- web: add table import form
- allow environment variable substitution in YAML configuration
- support for remote Delta tables (S3, ADLS)

### Changed
- cli: table uri lazy validation in app configuration
- web: table uri lazy validation in app configuration
- docs: update web application screenshots

### Fixed
- cli: laketower python entrypoint script
- always use quoted SQL identifiers in query builder

## [0.5.1] - 2025-05-30
Patch release with support for `deltalake` version 1.0.0.

### Changes
- deps: upgrade to `deltalake` version 1

## [0.5.0] - 2025-03-19
**Announcement:** Laketower open-source license is switching from AGPLv3 to Apache 2.0.

### Fixed
- deps: avoid dependency jinja2 version 3.1.5

### Changed
- docs: update configuration format
- docs: update web application section with screenshots

## [0.4.1] - 2025-03-02
Minor release with fixes.

### Added
- web: allow editing queries

### Fixed
- web: missing tables query page title
- web: urlencode table view sql query link

## [0.4.0] - 2025-03-01
Introducing new features:
- Display tables statistics
- List and execute pre-defined queries

### Added
- web: add queries view page
- web: add table statistics page with version control
- cli: add queries view command
- cli: add queries list command
- cli: add table statistics command

## [0.3.0] - 2025-02-27
Minor release with fixes and dropped Python 3.9 support.

### BREAKING CHANGES
- deps: drop support for python 3.9

### Fixed
- web: handle invalid tables sql query
- web: truncate long table names in sidebar

## [0.2.0] - 2025-02-25
Introducing the Laketower web application!

### Added
- `web` module
    - List all registered tables
    - Display table overview (metadata and schema)
    - Display table history
    - View a given table with simple query builder
    - Query all registered tables with DuckDB SQL dialect
- CLI: add `tables view --version` argument to time-travel table version

### Fixed
- Delta tables metadata compatibility when name and/or description is missing
- Delta tables history compatibility when created with Spark
- CLI: show default argument values in help

## [0.1.0] - 2025-02-15
Initial release of `laketower`.

### Added
- `cli` module
    - Validate YAML configuration
    - List all registered tables
    - Display a given table metadata
    - Display a given table schema
    - Display a given table history
    - View a given table with simple query builder
    - Query all registered tables with DuckDB SQL dialect

[Unreleased]: https://github.com/datalpia/laketower/compare/0.6.6...HEAD
[0.6.6]: https://github.com/datalpia/laketower/compare/0.6.5...0.6.6
[0.6.5]: https://github.com/datalpia/laketower/compare/0.6.4...0.6.5
[0.6.4]: https://github.com/datalpia/laketower/compare/0.6.3...0.6.4
[0.6.3]: https://github.com/datalpia/laketower/compare/0.6.2...0.6.3
[0.6.2]: https://github.com/datalpia/laketower/compare/0.6.1...0.6.2
[0.6.1]: https://github.com/datalpia/laketower/compare/0.6.0...0.6.1
[0.6.0]: https://github.com/datalpia/laketower/compare/0.5.1...0.6.0
[0.5.1]: https://github.com/datalpia/laketower/compare/0.5.0...0.5.1
[0.5.0]: https://github.com/datalpia/laketower/compare/0.4.1...0.5.0
[0.4.1]: https://github.com/datalpia/laketower/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/datalpia/laketower/compare/0.3.0...0.4.0
[0.3.0]: https://github.com/datalpia/laketower/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/datalpia/laketower/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/datalpia/laketower/releases/tag/0.1.0