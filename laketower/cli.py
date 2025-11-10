import argparse
import os
import time
from pathlib import Path

import rich.jupyter
import rich.panel
import rich.style
import rich.table
import rich.text
import rich.tree
import uvicorn
import pyarrow.csv as pacsv

from laketower.__about__ import __version__
from laketower.config import load_yaml_config
from laketower.tables import (
    ImportFileFormatEnum,
    ImportModeEnum,
    execute_query,
    extract_query_parameter_names,
    generate_table_query,
    generate_table_statistics_query,
    import_file_to_table,
    limit_query,
    load_datasets,
    load_table,
)


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
    out: rich.jupyter.JupyterMixin
    try:
        config = load_yaml_config(config_path)
        table_config = next(filter(lambda x: x.name == table_name, config.tables))
        table = load_table(table_config)
        metadata = table.metadata()

        out = rich.tree.Tree(table_name)
        out.add(f"name: {metadata.name}")
        out.add(f"description: {metadata.description}")
        out.add(f"format: {metadata.table_format.value}")
        out.add(f"uri: {metadata.uri}")
        out.add(f"id: {metadata.id}")
        out.add(f"version: {metadata.version}")
        out.add(f"created at: {metadata.created_at}")
        out.add(f"partitions: {', '.join(metadata.partitions)}")
        out.add(f"configuration: {metadata.configuration}")
    except Exception as e:
        out = rich.panel.Panel.fit(f"[red]{e}")

    console = rich.get_console()
    console.print(out)


def table_schema(config_path: Path, table_name: str) -> None:
    out: rich.jupyter.JupyterMixin
    try:
        config = load_yaml_config(config_path)
        table_config = next(filter(lambda x: x.name == table_name, config.tables))
        table = load_table(table_config)
        schema = table.schema()

        out = rich.tree.Tree(table_name)
        for field in schema:
            nullable = "" if field.nullable else " not null"
            out.add(f"{field.name}: {field.type}{nullable}")
    except Exception as e:
        out = rich.panel.Panel.fit(f"[red]{e}")

    console = rich.get_console()
    console.print(out, markup=False)  # disable markup to allow bracket characters


def table_history(config_path: Path, table_name: str) -> None:
    out: rich.jupyter.JupyterMixin
    try:
        config = load_yaml_config(config_path)
        table_config = next(filter(lambda x: x.name == table_name, config.tables))
        table = load_table(table_config)
        history = table.history()

        out = rich.tree.Tree(table_name)
        for rev in history.revisions:
            tree_version = out.add(f"version: {rev.version}")
            tree_version.add(f"timestamp: {rev.timestamp}")
            tree_version.add(f"client version: {rev.client_version}")
            tree_version.add(f"operation: {rev.operation}")
            tree_op_params = tree_version.add("operation parameters")
            for param_key, param_val in rev.operation_parameters.items():
                tree_op_params.add(f"{param_key}: {param_val}")
            tree_op_metrics = tree_version.add("operation metrics")
            for metric_key, metric_val in rev.operation_metrics.items():
                tree_op_metrics.add(f"{metric_key}: {metric_val}")
    except Exception as e:
        out = rich.panel.Panel.fit(f"[red]{e}")

    console = rich.get_console()
    console.print(out, markup=False)


def table_statistics(
    config_path: Path, table_name: str, version: int | None = None
) -> None:
    out: rich.jupyter.JupyterMixin
    try:
        config = load_yaml_config(config_path)
        table_config = next(filter(lambda x: x.name == table_name, config.tables))
        table = load_table(table_config)
        table_dataset = table.dataset(version=version)
        sql_query = generate_table_statistics_query(table_name)
        results = execute_query({table_name: table_dataset}, sql_query)

        out = rich.table.Table()
        for column in results.column_names:
            out.add_column(column)
        for row_dict in results.to_pylist():
            out.add_row(*[str(row_dict[col]) for col in results.column_names])
    except Exception as e:
        out = rich.panel.Panel.fit(f"[red]{e}")

    console = rich.get_console()
    console.print(out, markup=False)  # disable markup to allow bracket characters


def view_table(
    config_path: Path,
    table_name: str,
    limit: int | None = None,
    cols: list[str] | None = None,
    sort_asc: str | None = None,
    sort_desc: str | None = None,
    version: int | None = None,
) -> None:
    out: rich.jupyter.JupyterMixin
    try:
        config = load_yaml_config(config_path)
        table_config = next(filter(lambda x: x.name == table_name, config.tables))
        table = load_table(table_config)
        table_dataset = table.dataset(version=version)
        sql_query = generate_table_query(
            table_name, limit=limit, cols=cols, sort_asc=sort_asc, sort_desc=sort_desc
        )
        results = execute_query({table_name: table_dataset}, sql_query)

        out = rich.table.Table()
        for column in results.column_names:
            out.add_column(column)
        for row_dict in results.to_pylist():
            out.add_row(*[str(row_dict[col]) for col in results.column_names])
    except Exception as e:
        out = rich.panel.Panel.fit(f"[red]{e}")

    console = rich.get_console()
    console.print(out)


def query_table(
    config_path: Path,
    sql_query: str,
    sql_params: list[list[str]] = [],
    output_path: Path | None = None,
) -> None:
    out: rich.jupyter.JupyterMixin
    try:
        config = load_yaml_config(config_path)
        tables_dataset = load_datasets(config.tables)
        sql_params_dict = {param[0]: param[1] for param in sql_params}
        query_param_names = extract_query_parameter_names(sql_query)
        query_params = {
            name: sql_params_dict.get(name) or "" for name in query_param_names
        }
        limited_sql_query = limit_query(sql_query, config.settings.max_query_rows + 1)

        start_time = time.perf_counter()
        results = execute_query(
            tables_dataset, limited_sql_query, sql_params=query_params
        )
        execution_time_ms = (time.perf_counter() - start_time) * 1000

        truncated = results.num_rows > config.settings.max_query_rows
        results = results.slice(
            0, min(results.num_rows, config.settings.max_query_rows)
        )

        out = rich.table.Table(
            caption=(
                f"{results.num_rows} rows returned{' (truncated)' if truncated else ''}"
                f"\nExecution time: {execution_time_ms:.2f}ms"
            ),
            caption_justify="left",
            caption_style=rich.style.Style(dim=True),
        )
        for column in results.column_names:
            out.add_column(column)
        for row_dict in results.to_pylist():
            out.add_row(*[str(row_dict[col]) for col in results.column_names])

        if output_path is not None:
            pacsv.write_csv(
                results,
                output_path,
                pacsv.WriteOptions(include_header=True, delimiter=","),
            )
            out = rich.text.Text(f"Query results written to: {output_path}")
    except ValueError as e:
        out = rich.panel.Panel.fit(f"[red]{e}")

    console = rich.get_console()
    console.print(out)


def import_table(
    config_path: Path,
    table_name: str,
    file_path: Path,
    mode: ImportModeEnum,
    file_format: ImportFileFormatEnum,
    delimiter: str,
    encoding: str,
) -> None:
    out: rich.jupyter.JupyterMixin
    try:
        config = load_yaml_config(config_path)
        table_config = next(filter(lambda x: x.name == table_name, config.tables))
        with open(file_path, "rb") as file_content:
            rows_imported = import_file_to_table(
                table_config, file_content, mode, file_format, delimiter, encoding
            )
        out = rich.text.Text(
            f"Successfully imported {rows_imported} rows into table '{table_name}' in '{mode.value}' mode"
        )
    except Exception as e:
        out = rich.panel.Panel.fit(f"[red]{e}")

    console = rich.get_console()
    console.print(out)


def list_queries(config_path: Path) -> None:
    config = load_yaml_config(config_path)
    tree = rich.tree.Tree("queries")
    for query in config.queries:
        tree.add(query.name)
    console = rich.get_console()
    console.print(tree)


def view_query(
    config_path: Path, query_name: str, query_params: list[list[str]] = []
) -> None:
    out: rich.jupyter.JupyterMixin
    try:
        config = load_yaml_config(config_path)
        tables_dataset = load_datasets(config.tables)
        query_config = next(filter(lambda x: x.name == query_name, config.queries))
        default_parameters = {k: v.default for k, v in query_config.parameters.items()}
        sql_query = query_config.sql
        query_params_dict = {param[0]: param[1] for param in query_params}
        sql_param_names = extract_query_parameter_names(sql_query)
        sql_params = {
            name: query_params_dict.get(name) or default_parameters.get(name) or ""
            for name in sql_param_names
        }
        limited_sql_query = limit_query(sql_query, config.settings.max_query_rows + 1)

        start_time = time.perf_counter()
        results = execute_query(
            tables_dataset, limited_sql_query, sql_params=sql_params
        )
        execution_time_ms = (time.perf_counter() - start_time) * 1000

        truncated = results.num_rows > config.settings.max_query_rows
        results = results.slice(
            0, min(results.num_rows, config.settings.max_query_rows)
        )

        out = rich.table.Table(
            caption=(
                f"{results.num_rows} rows returned{' (truncated)' if truncated else ''}"
                f"\nExecution time: {execution_time_ms:.2f}ms"
            ),
            caption_justify="left",
            caption_style=rich.style.Style(dim=True),
        )
        for column in results.column_names:
            out.add_column(column)
        for row_dict in results.to_pylist():
            out.add_row(*[str(row_dict[col]) for col in results.column_names])
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

    parser_tables_statistics = subsparsers_tables.add_parser(
        "statistics", help="Display summary statistics of a given table schema"
    )
    parser_tables_statistics.add_argument("table", help="Name of the table")
    parser_tables_statistics.add_argument(
        "--version", type=int, help="Time-travel to table revision number"
    )
    parser_tables_statistics.set_defaults(
        func=lambda x: table_statistics(x.config, x.table, x.version)
    )

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
    parser_tables_query.add_argument(
        "--output", help="Output query results to a file (default format: CSV)"
    )
    parser_tables_query.add_argument(
        "--param",
        "-p",
        nargs=2,
        action="append",
        default=[],
        help="Inject query named parameters values",
    )
    parser_tables_query.add_argument("sql", help="SQL query to execute")
    parser_tables_query.set_defaults(
        func=lambda x: query_table(x.config, x.sql, x.param, x.output)
    )

    parser_tables_import = subsparsers_tables.add_parser(
        "import", help="Import data into a table"
    )
    parser_tables_import.add_argument("table", help="Name of the table")
    parser_tables_import.add_argument(
        "--file", type=Path, required=True, help="Path to file to import"
    )
    parser_tables_import.add_argument(
        "--mode",
        choices=[mode.value for mode in ImportModeEnum],
        default=ImportModeEnum.append.value,
        type=ImportModeEnum,
        help=f"Import mode (default: {ImportModeEnum.append.value})",
    )
    parser_tables_import.add_argument(
        "--format",
        choices=[file_format.value for file_format in ImportFileFormatEnum],
        default=ImportFileFormatEnum.csv.value,
        type=ImportFileFormatEnum,
        help=f"File format (default: {ImportFileFormatEnum.csv.value})",
    )
    parser_tables_import.add_argument(
        "--delimiter", default=",", help="Column delimiter to use (default: ',')"
    )
    parser_tables_import.add_argument(
        "--encoding", default="utf-8", help="File encoding to use (default: 'utf-8')"
    )
    parser_tables_import.set_defaults(
        func=lambda x: import_table(
            x.config, x.table, x.file, x.mode, x.format, x.delimiter, x.encoding
        )
    )

    parser_queries = subparsers.add_parser("queries", help="Work with queries")
    subsparsers_queries = parser_queries.add_subparsers(required=True)

    parser_queries_list = subsparsers_queries.add_parser(
        "list", help="List all registered queries"
    )
    parser_queries_list.set_defaults(func=lambda x: list_queries(x.config))

    parser_queries_view = subsparsers_queries.add_parser(
        "view", help="View a given query"
    )
    parser_queries_view.add_argument("query", help="Name of the query")
    parser_queries_view.add_argument(
        "--param",
        "-p",
        nargs=2,
        action="append",
        default=[],
        help="Inject query named parameters values",
    )
    parser_queries_view.set_defaults(
        func=lambda x: view_query(x.config, x.query, x.param)
    )

    args = parser.parse_args()
    args.func(args)
