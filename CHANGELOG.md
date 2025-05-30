# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/datalpia/laketower/compare/0.5.1...HEAD
[0.5.1]: https://github.com/datalpia/laketower/compare/0.5.0...0.5.1
[0.5.0]: https://github.com/datalpia/laketower/compare/0.4.1...0.5.0
[0.4.1]: https://github.com/datalpia/laketower/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/datalpia/laketower/compare/0.3.0...0.4.0
[0.3.0]: https://github.com/datalpia/laketower/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/datalpia/laketower/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/datalpia/laketower/releases/tag/0.1.0