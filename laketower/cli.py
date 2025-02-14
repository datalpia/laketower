from __future__ import annotations

import argparse
import enum
from datetime import datetime, timezone
from pathlib import Path

import deltalake
import duckdb
import pandas as pd
import pyarrow as pa
import pydantic
import rich.panel
import rich.table
import rich.text
import rich.tree
import sqlglot
import sqlglot.dialects
import sqlglot.dialects.duckdb
import sqlglot.generator
import yaml


class TableFormats(str, enum.Enum):
    delta = "delta"


class ConfigTable(pydantic.BaseModel):
    name: str
    uri: str
    table_format: TableFormats = pydantic.Field(alias="format")

    @pydantic.model_validator(mode="after")
    def check_table(self) -> "ConfigTable":
        def check_delta_table(table_uri: str) -> None:
            if not deltalake.DeltaTable.is_deltatable(table_uri):
                raise ValueError(f"{table_uri} is not a valid Delta table")

        format_check = {TableFormats.delta: check_delta_table}
        format_check[self.table_format](self.uri)

        return self


class ConfigQuery(pydantic.BaseModel):
    name: str
    sql: str


class ConfigDashboard(pydantic.BaseModel):
    name: str


class Config(pydantic.BaseModel):
    tables: list[ConfigTable] = []


def load_yaml_config(config_path: Path) -> Config:
    config_dict = yaml.safe_load(config_path.read_text())
    return Config.model_validate(config_dict)


class TableMetadata(pydantic.BaseModel):
    table_format: TableFormats
    name: str
    description: str
    uri: str
    id: str
    version: int
    created_at: datetime
    partitions: list[str]
    configuration: dict[str, str]


def load_table_metadata(table_config: ConfigTable) -> TableMetadata:
    def load_delta_table_metadata(table_config: ConfigTable) -> TableMetadata:
        delta_table = deltalake.DeltaTable(table_config.uri)
        metadata = delta_table.metadata()
        return TableMetadata(
            table_format=table_config.table_format,
            name=metadata.name,
            description=metadata.description,
            uri=delta_table.table_uri,
            id=str(metadata.id),
            version=delta_table.version(),
            created_at=datetime.fromtimestamp(
                metadata.created_time / 1000, tz=timezone.utc
            ),
            partitions=metadata.partition_columns,
            configuration=metadata.configuration,
        )

    format_handler = {TableFormats.delta: load_delta_table_metadata}
    return format_handler[table_config.table_format](table_config)


def load_table_schema(table_config: ConfigTable) -> pa.Schema:
    def load_delta_table_schema(table_config: ConfigTable) -> pa.Schema:
        delta_table = deltalake.DeltaTable(table_config.uri)
        return delta_table.schema().to_pyarrow()

    schema_handler = {TableFormats.delta: load_delta_table_schema}
    return schema_handler[table_config.table_format](table_config)


def execute_query_table(table_config: ConfigTable, sql_query: str) -> pd.DataFrame:
    def execute_delta_table_query(table_config: ConfigTable) -> pd.DataFrame:
        delta_table = deltalake.DeltaTable(table_config.uri)
        table_name = table_config.name
        view_name = f"{table_name}_view"
        try:
            conn = duckdb.connect()
            conn.register(view_name, delta_table.to_pyarrow_dataset())
            conn.execute(f"create table {table_name} as select * from {view_name}")  # nosec B608
            return conn.execute(sql_query).df()
        except duckdb.Error as e:
            raise ValueError(str(e)) from e

    query_handler = {TableFormats.delta: execute_delta_table_query}
    return query_handler[table_config.table_format](table_config)


def validate_config(config_path: Path) -> None:
    console = rich.get_console()
    try:
        config = load_yaml_config(config_path)
        console.print(rich.text.Text("Configuration is valid"))
        console.print(config)
    except Exception as e:
        console.print(rich.text.Text("Configuration is invalid"))
        console.print(e)


def list_tables(config_path: Path) -> None:
    config = load_yaml_config(config_path)
    tree = rich.tree.Tree("tables")
    for table in config.tables:
        table_tree = tree.add(table.name)
        table_tree.add(f"format: {table.table_format.value}")
        table_tree.add(f"uri: {table.uri}")
    console = rich.get_console()
    console.print(tree)


def inspect_table(config_path: Path, table_name: str) -> None:
    config = load_yaml_config(config_path)
    table_config = next(filter(lambda x: x.name == table_name, config.tables))
    metadata = load_table_metadata(table_config)

    tree = rich.tree.Tree(table_name)
    tree.add(f"name: {metadata.name}")
    tree.add(f"description: {metadata.description}")
    tree.add(f"format: {metadata.table_format.value}")
    tree.add(f"uri: {metadata.uri}")
    tree.add(f"id: {metadata.id}")
    tree.add(f"version: {metadata.version}")
    tree.add(f"created at: {metadata.created_at}")
    tree.add(f"partitions: {', '.join(metadata.partitions)}")
    tree.add(f"configuration: {metadata.configuration}")
    console = rich.get_console()
    console.print(tree)


def view_table(
    config_path: Path,
    table_name: str,
    limit: int | None = None,
    cols: list[str] | None = None,
    sort_asc: str | None = None,
    sort_desc: str | None = None,
) -> None:
    config = load_yaml_config(config_path)
    table_config = next(filter(lambda x: x.name == table_name, config.tables))

    query_expr = sqlglot.select(*(cols or ["*"])).from_(table_name).limit(limit or 10)
    if sort_asc:
        query_expr = query_expr.order_by(f"{sort_asc} asc")
    elif sort_desc:
        query_expr = query_expr.order_by(f"{sort_desc} desc")
    sql_query = sqlglot.Generator(dialect=sqlglot.dialects.DuckDB).generate(query_expr)

    results = execute_query_table(table_config, sql_query)
    out = rich.table.Table()
    for column in results.columns:
        out.add_column(column)
    for value_list in results.values.tolist():
        row = [str(x) for x in value_list]
        out.add_row(*row)

    console = rich.get_console()
    console.print(out)


def query_table(config_path: Path, table_name: str, sql_query: str | None) -> None:
    if not sql_query:
        sql_query = f"select * from {table_name} limit 10"  # nosec B608

    config = load_yaml_config(config_path)
    table_config = next(filter(lambda x: x.name == table_name, config.tables))
    _ = load_table_schema(table_config)

    out: rich.jupyter.JupyterMixin
    try:
        results = execute_query_table(table_config, sql_query)
        out = rich.table.Table()
        for column in results.columns:
            out.add_column(column)
        for value_list in results.values.tolist():
            row = [str(x) for x in value_list]
            out.add_row(*row)
    except ValueError as e:
        out = rich.panel.Panel.fit(f"[red]{e}")

    console = rich.get_console()
    console.print(out)


def cli() -> None:
    parser = argparse.ArgumentParser("laketower")
    parser.add_argument(
        "--config",
        "-c",
        default="laketower.yml",
        type=Path,
        help="Path to the Laketower YAML configuration file",
    )
    subparsers = parser.add_subparsers(title="commands", required=True)

    parser_config = subparsers.add_parser(
        "config", help="Work with configuration", add_help=True
    )
    subsparsers_config = parser_config.add_subparsers(required=True)

    parser_config_validate = subsparsers_config.add_parser("validate")
    parser_config_validate.set_defaults(func=lambda x: validate_config(x.config))

    parser_tables = subparsers.add_parser("tables", help="Work with tables")
    subsparsers_tables = parser_tables.add_subparsers(required=True)

    parser_tables_list = subsparsers_tables.add_parser(
        "list", help="List all registered tables"
    )
    parser_tables_list.set_defaults(func=lambda x: list_tables(x.config))

    parser_tables_inspect = subsparsers_tables.add_parser(
        "inspect", help="Inspect a given table"
    )
    parser_tables_inspect.add_argument("table", help="Name of the table")
    parser_tables_inspect.set_defaults(func=lambda x: inspect_table(x.config, x.table))

    parser_tables_view = subsparsers_tables.add_parser(
        "view", help="View a given table"
    )
    parser_tables_view.add_argument("table", help="Name of the table")
    parser_tables_view.add_argument(
        "--limit", type=int, help="Maximum number of rows to display"
    )
    parser_tables_view.add_argument("--cols", nargs="*", help="Columns to display")
    parser_tables_view_sort_group = parser_tables_view.add_mutually_exclusive_group()
    parser_tables_view_sort_group.add_argument(
        "--sort-asc", help="Sort by given column in ascending order"
    )
    parser_tables_view_sort_group.add_argument(
        "--sort-desc", help="Sort by given column in descending order"
    )
    parser_tables_view.set_defaults(
        func=lambda x: view_table(
            x.config, x.table, x.limit, x.cols, x.sort_asc, x.sort_desc
        )
    )

    parser_tables_query = subsparsers_tables.add_parser(
        "query", help="Query a given table"
    )
    parser_tables_query.add_argument("table", help="Name of the table")
    parser_tables_query.add_argument("--sql", help="SQL query to execute")
    parser_tables_query.set_defaults(
        func=lambda x: query_table(x.config, x.table, x.sql)
    )

    args = parser.parse_args()
    args.func(args)
