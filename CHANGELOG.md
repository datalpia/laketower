# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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

[Unreleased]: https://github.com/datalpia/laketower/compare/0.1.0...HEAD
[0.1.0]: https://github.com/datalpia/laketower/releases/tag/0.1.0