from __future__ import annotations

import argparse
import os
from pathlib import Path

import rich.jupyter
import rich.panel
import rich.table
import rich.text
import rich.tree
import uvicorn

from laketower.__about__ import __version__
from laketower.config import load_yaml_config
from laketower.tables import execute_query, generate_table_query, load_table


def run_web(config_path: Path, reload: bool) -> None:  # pragma: no cover
    os.environ["LAKETOWER_CONFIG_PATH"] = str(config_path.absolute())
    uvicorn.run("laketower.web:create_app", factory=True, reload=reload)


def validate_config(config_path: Path) -> None:
    console = rich.get_console()
    try:
        config = load_yaml_config(config_path)
        console.print(rich.panel.Panel.fit("[green]Configuration is valid"))
        console.print(config)
    except Exception as e:
        console.print(rich.panel.Panel.fit("[red]Configuration is invalid"))
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


def table_metadata(config_path: Path, table_name: str) -> None:
    config = load_yaml_config(config_path)
    table_config = next(filter(lambda x: x.name == table_name, config.tables))
    table = load_table(table_config)
    metadata = table.metadata()

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


def table_schema(config_path: Path, table_name: str) -> None:
    config = load_yaml_config(config_path)
    table_config = next(filter(lambda x: x.name == table_name, config.tables))
    table = load_table(table_config)
    schema = table.schema()

    tree = rich.tree.Tree(table_name)
    for field in schema:
        nullable = "" if field.nullable else " not null"
        tree.add(f"{field.name}: {field.type}{nullable}")
    console = rich.get_console()
    console.print(tree, markup=False)  # disable markup to allow bracket characters


def table_history(config_path: Path, table_name: str) -> None:
    config = load_yaml_config(config_path)
    table_config = next(filter(lambda x: x.name == table_name, config.tables))
    table = load_table(table_config)
    history = table.history()

    tree = rich.tree.Tree(table_name)
    for rev in history.revisions:
        tree_version = tree.add(f"version: {rev.version}")
        tree_version.add(f"timestamp: {rev.timestamp}")
        tree_version.add(f"client version: {rev.client_version}")
        tree_version.add(f"operation: {rev.operation}")
        tree_op_params = tree_version.add("operation parameters")
        for param_key, param_val in rev.operation_parameters.items():
            tree_op_params.add(f"{param_key}: {param_val}")
        tree_op_metrics = tree_version.add("operation metrics")
        for metric_key, metric_val in rev.operation_metrics.items():
            tree_op_metrics.add(f"{metric_key}: {metric_val}")
    console = rich.get_console()
    console.print(tree, markup=False)


def view_table(
    config_path: Path,
    table_name: str,
    limit: int | None = None,
    cols: list[str] | None = None,
    sort_asc: str | None = None,
    sort_desc: str | None = None,
    version: int | None = None,
) -> None:
    config = load_yaml_config(config_path)
    table_config = next(filter(lambda x: x.name == table_name, config.tables))
    table = load_table(table_config)
    table_dataset = table.dataset(version=version)
    sql_query = generate_table_query(
        table_name, limit=limit, cols=cols, sort_asc=sort_asc, sort_desc=sort_desc
    )
    results = execute_query({table_name: table_dataset}, sql_query)

    out = rich.table.Table()
    for column in results.columns:
        out.add_column(column)
    for value_list in results.to_numpy().tolist():
        row = [str(x) for x in value_list]
        out.add_row(*row)

    console = rich.get_console()
    console.print(out)


def query_table(config_path: Path, sql_query: str) -> None:
    config = load_yaml_config(config_path)
    tables_dataset = {
        table_config.name: load_table(table_config).dataset()
        for table_config in config.tables
    }

    out: rich.jupyter.JupyterMixin
    try:
        results = execute_query(tables_dataset, sql_query)
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
    parser = argparse.ArgumentParser(
        "laketower", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument(
        "--config",
        "-c",
        default="laketower.yml",
        type=Path,
        help="Path to the Laketower YAML configuration file",
    )
    subparsers = parser.add_subparsers(title="commands", required=True)

    parser_web = subparsers.add_parser(
        "web", help="Launch the web application", add_help=True
    )
    parser_web.add_argument(
        "--reload",
        help="Reload the web server on changes",
        action="store_true",
        required=False,
    )
    parser_web.set_defaults(func=lambda x: run_web(x.config, x.reload))

    parser_config = subparsers.add_parser(
        "config", help="Work with configuration", add_help=True
    )
    subsparsers_config = parser_config.add_subparsers(required=True)

    parser_config_validate = subsparsers_config.add_parser(
        "validate", help="Validate YAML configuration"
    )
    parser_config_validate.set_defaults(func=lambda x: validate_config(x.config))

    parser_tables = subparsers.add_parser("tables", help="Work with tables")
    subsparsers_tables = parser_tables.add_subparsers(required=True)

    parser_tables_list = subsparsers_tables.add_parser(
        "list", help="List all registered tables"
    )
    parser_tables_list.set_defaults(func=lambda x: list_tables(x.config))

    parser_tables_metadata = subsparsers_tables.add_parser(
        "metadata", help="Display a given table metadata"
    )
    parser_tables_metadata.add_argument("table", help="Name of the table")
    parser_tables_metadata.set_defaults(
        func=lambda x: table_metadata(x.config, x.table)
    )

    parser_tables_schema = subsparsers_tables.add_parser(
        "schema", help="Display a given table schema"
    )
    parser_tables_schema.add_argument("table", help="Name of the table")
    parser_tables_schema.set_defaults(func=lambda x: table_schema(x.config, x.table))

    parser_tables_history = subsparsers_tables.add_parser(
        "history", help="Display the history of a given table schema"
    )
    parser_tables_history.add_argument("table", help="Name of the table")
    parser_tables_history.set_defaults(func=lambda x: table_history(x.config, x.table))

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
    parser_tables_view.add_argument(
        "--version", type=int, help="Time-travel to table revision number"
    )
    parser_tables_view.set_defaults(
        func=lambda x: view_table(
            x.config, x.table, x.limit, x.cols, x.sort_asc, x.sort_desc, x.version
        )
    )

    parser_tables_query = subsparsers_tables.add_parser(
        "query", help="Query registered tables"
    )
    parser_tables_query.add_argument("sql", help="SQL query to execute")
    parser_tables_query.set_defaults(func=lambda x: query_table(x.config, x.sql))

    args = parser.parse_args()
    args.func(args)
