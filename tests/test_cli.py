import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import deltalake
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pytest
import yaml

from laketower import cli


def test_version(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from laketower.__about__ import __version__

    monkeypatch.setattr(
        sys,
        "argv",
        ["laketower", "--version"],
    )

    with pytest.raises(SystemExit):
        cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert __version__ in output


def test_config_validate(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["laketower", "--config", str(sample_config_path), "config", "validate"],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert "Configuration is valid" in output


def test_config_validate_unknown_path(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        sys, "argv", ["laketower", "--config", "unknown.yml", "config", "validate"]
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert "Configuration is invalid" in output


def test_config_validate_invalid_table_format(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    sample_config: dict[str, Any],
) -> None:
    sample_config["tables"][0]["format"] = "unknown_format"
    sample_config_path = tmp_path / "laketower.yml"
    sample_config_path.write_text(yaml.dump(sample_config))

    monkeypatch.setattr(
        sys,
        "argv",
        ["laketower", "--config", str(sample_config_path), "config", "validate"],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert "Configuration is invalid" in output


def test_tables_list(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["laketower", "--config", str(sample_config_path), "tables", "list"],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    for table in sample_config["tables"]:
        assert table["name"] in output
        assert Path(table["uri"]).name in output
        assert f"format: {table['format']}" in output


def test_tables_metadata(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "metadata",
            sample_config["tables"][0]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert "format: delta" in output
    assert f"name: {delta_table.metadata().name}" in output
    assert f"description: {delta_table.metadata().description}" in output
    assert "uri:" in output
    assert f"id: {delta_table.metadata().id}" in output
    assert f"version: {delta_table.version()}" in output
    assert (
        f"created at: {datetime.fromtimestamp(delta_table.metadata().created_time / 1000, tz=timezone.utc)}"
        in output
    )
    assert (
        f"partitions: {', '.join(delta_table.metadata().partition_columns)}" in output
    )
    assert f"configuration: {delta_table.metadata().configuration}" in output


def test_tables_metadata_invalid_table_uri(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "metadata",
            sample_config["tables"][-1]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert f"Invalid table: {sample_config['tables'][-1]['uri']}" in output


def test_tables_schema(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    table_name = sample_config["tables"][0]["name"]

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "schema",
            table_name,
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert output.startswith(table_name)

    table_schema = pa.schema(delta_table.schema().to_arrow())  # type: ignore[arg-type]
    for field in table_schema:
        nullable = "" if field.nullable else " not null"
        assert f"{field.name}: {field.type}{nullable}" in output


def test_tables_schema_invalid_table_uri(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "schema",
            sample_config["tables"][-1]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert f"Invalid table: {sample_config['tables'][-1]['uri']}" in output


def test_tables_history(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    table_name = sample_config["tables"][0]["name"]

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "history",
            table_name,
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert output.startswith(table_name)
    for rev in delta_table.history():
        assert f"version: {rev['version']}" in output
        assert (
            f"timestamp: {datetime.fromtimestamp(rev['timestamp'] / 1000, tz=timezone.utc)}"
            in output
        )
        assert f"client version: {rev['clientVersion']}" in output
        assert f"operation: {rev['operation']}" in output
        assert "operation parameters" in output
        operation_parameters = rev["operationParameters"]
        for param_key in operation_parameters.keys():
            assert f"{param_key}: " in output
        assert "operation metrics" in output
        operation_metrics = rev.get("operationMetrics")
        if operation_metrics:
            for metric_key, metric_val in operation_metrics.items():
                assert f"{metric_key}: {metric_val}" in output


def test_tables_history_invalid_table_uri(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "history",
            sample_config["tables"][-1]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert f"Invalid table: {sample_config['tables'][-1]['uri']}" in output


def test_tables_statistics(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "statistics",
            sample_config["tables"][0]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert all(field.name in output for field in delta_table.schema().fields)
    assert "column_name" in output
    assert "count" in output
    assert "avg" in output
    assert "std" in output
    assert "min" in output
    assert "max" in output


def test_tables_statistics_version(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "statistics",
            "--version",
            "0",
            sample_config["tables"][0]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert all(field.name in output for field in delta_table.schema().fields)
    assert "column_name" in output
    assert "count" in output
    assert "avg" in output
    assert "std" in output
    assert "min" in output
    assert "max" in output


def test_tables_statistics_invalid_table_uri(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "statistics",
            sample_config["tables"][-1]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert f"Invalid table: {sample_config['tables'][-1]['uri']}" in output


def test_tables_view(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    default_limit = 10

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "view",
            sample_config["tables"][0]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert all(field.name in output for field in delta_table.schema().fields)

    df = delta_table.to_pandas()[0:default_limit]
    assert all(str(row[col]) in output for _, row in df.iterrows() for col in row.index)


def test_tables_view_limit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    selected_limit = 1

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "view",
            sample_config["tables"][0]["name"],
            "--limit",
            str(selected_limit),
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert all(field.name in output for field in delta_table.schema().fields)

    df = delta_table.to_pandas()[0:selected_limit]
    assert all(str(row[col]) in output for _, row in df.iterrows() for col in row.index)


def test_tables_view_cols(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    default_limit = 10
    num_fields = len(delta_table.schema().fields)
    selected_columns = [
        delta_table.schema().fields[i].name for i in range(num_fields - 1)
    ]
    filtered_columns = [delta_table.schema().fields[num_fields - 1].name]

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "view",
            sample_config["tables"][0]["name"],
            "--cols",
            *selected_columns,
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert all(col in output for col in selected_columns)
    assert not all(col in output for col in filtered_columns)

    df = delta_table.to_pandas()[:default_limit]
    assert all(
        str(row[col]) in output
        for _, row in df[selected_columns].iterrows()
        for col in row.index
    )
    assert not all(
        str(row[col]) in output
        for _, row in df[filtered_columns].iterrows()
        for col in row.index
    )


def test_tables_view_sort_asc(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    default_limit = 10
    sort_column = "temperature"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "view",
            sample_config["tables"][0]["name"],
            "--sort-asc",
            sort_column,
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert all(field.name in output for field in delta_table.schema().fields)

    df = delta_table.to_pandas().sort_values(by=sort_column, ascending=True)[
        :default_limit
    ]
    assert all(str(row[col]) in output for _, row in df.iterrows() for col in row.index)


def test_tables_view_sort_desc(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    default_limit = 10
    sort_column = "temperature"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "view",
            sample_config["tables"][0]["name"],
            "--sort-desc",
            sort_column,
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert all(field.name in output for field in delta_table.schema().fields)

    df = delta_table.to_pandas().sort_values(by=sort_column, ascending=False)[
        :default_limit
    ]
    assert all(str(row[col]) in output for _, row in df.iterrows() for col in row.index)


def test_tables_view_version(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    default_limit = 10

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "view",
            "--version",
            "0",
            sample_config["tables"][0]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert all(field.name in output for field in delta_table.schema().fields)

    df = delta_table.to_pandas()[0:default_limit]
    assert not all(
        str(row[col]) in output for _, row in df.iterrows() for col in row.index
    )


def test_tables_view_invalid_table_uri(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "view",
            sample_config["tables"][-1]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert f"Invalid table: {sample_config['tables'][-1]['uri']}" in output


def test_tables_query(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    selected_column = delta_table.schema().fields[0].name
    filtered_columns = [field.name for field in delta_table.schema().fields[1:]]
    selected_limit = 1

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "query",
            f"select {selected_column} from {sample_config['tables'][0]['name']} limit {selected_limit}",
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert f"{selected_limit} rows returned" in output
    assert "Execution time: " in output
    assert selected_column in output
    assert not all(col in output for col in filtered_columns)

    df = delta_table.to_pandas()
    assert all(str(row) in output for row in df[selected_column][0:selected_limit])
    assert not all(str(row) in output for row in df[selected_column][selected_limit:])


def test_tables_query_max_rows_limit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    max_query_rows = 3
    sample_config["settings"]["max_query_rows"] = max_query_rows
    sample_config_path.write_text(yaml.dump(sample_config))

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "query",
            f"select * from {sample_config['tables'][0]['name']}",
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert f"{max_query_rows} rows returned (truncated)" in output
    assert "Execution time: " in output


def test_tables_query_parameters(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    selected_column = delta_table.schema().fields[0].name
    filtered_columns = [field.name for field in delta_table.schema().fields[1:]]
    param_column = "time"
    start_date = "2025-01-01"
    end_date = "2025-01-31"
    selected_limit = 1

    sql_query = f"select {selected_column} from {sample_config['tables'][0]['name']} where {param_column} between $start_date and $end_date limit {selected_limit}"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "query",
            sql_query,
            "--param",
            "start_date",
            start_date,
            "--param",
            "end_date",
            end_date,
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert selected_column in output
    assert not all(col in output for col in filtered_columns)

    df = delta_table.to_pandas()
    df[(df[param_column] == start_date) & (df[param_column] == end_date)]
    assert all(str(row) in output for row in df[selected_column][0:selected_limit])
    assert not all(str(row) in output for row in df[selected_column][selected_limit:])


def test_tables_query_output_csv(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    selected_column = delta_table.schema().fields[0].name
    selected_limit = 3

    output_csv_path = tmp_path / "output.csv"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "query",
            "--output",
            str(output_csv_path),
            f"select {selected_column} from {sample_config['tables'][0]['name']} limit {selected_limit}",
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert "Query results written to:" in output
    assert output_csv_path.name in output
    assert output_csv_path.exists()

    df = delta_table.to_pandas()
    expected_output = df[[selected_column]][0:selected_limit]
    expected_csv_path = tmp_path / "expected.csv"

    expected_table = pa.Table.from_pandas(expected_output)
    pacsv.write_csv(
        expected_table,
        expected_csv_path,
        pacsv.WriteOptions(include_header=True, delimiter=","),
    )
    assert output_csv_path.read_text() == expected_csv_path.read_text()


@pytest.mark.parametrize(
    ["sql"],
    [
        ("select * from unknown_table",),
        ("select * from invalid_table_uri",),
        ("select",),
        ("",),
    ],
)
def test_tables_query_invalid_sql(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
    sql: str,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "query",
            sql,
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert "Error" in output
    assert not all(field.name in output for field in delta_table.schema().fields)

    df = delta_table.to_pandas()
    assert not all(
        str(row[col]) in output for _, row in df.iterrows() for col in row.index
    )


def test_queries_list(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "queries",
            "list",
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert "queries" in output
    for query in sample_config["queries"]:
        assert query["name"] in output


def test_queries_view(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "queries",
            "view",
            sample_config["queries"][0]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert " rows returned" in output
    assert "Execution time: " in output
    assert all(col in output for col in {"day", "avg_temperature"})


def test_queries_view_max_row_limit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    max_query_rows = 3
    sample_config["settings"]["max_query_rows"] = max_query_rows
    sample_config_path.write_text(yaml.dump(sample_config))

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "queries",
            "view",
            sample_config["queries"][0]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert f"{max_query_rows} rows returned (truncated)" in output
    assert "Execution time: " in output
    assert all(col in output for col in {"day", "avg_temperature"})


def test_queries_view_parameters_default(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "queries",
            "view",
            sample_config["queries"][1]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert all(col in output for col in {"day", "avg_temperature"})


def test_queries_view_parameters(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "queries",
            "view",
            sample_config["queries"][1]["name"],
            "--param",
            "start_date",
            "2025-01-01",
            "--param",
            "end_date",
            "2025-01-31",
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert all(col in output for col in {"day", "avg_temperature"})


def test_queries_view_invalid_sql(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    sample_config["queries"][0]["sql"] = "select * from unknown_table"
    sample_config_path.write_text(yaml.dump(sample_config))

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "queries",
            "view",
            sample_config["queries"][0]["name"],
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert "Error" in output
    assert not all(col in output for col in {"day", "avg_temperature"})


@pytest.mark.parametrize("delimiter", [",", ";"])
@pytest.mark.parametrize("encoding", ["utf-8", "latin-1"])
def test_tables_import_csv_append(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
    delimiter: str,
    encoding: str,
) -> None:
    table_name = sample_config["tables"][0]["name"]
    insert_mode = "append"

    new_city = "Lyon"
    csv_data = pd.DataFrame(
        {
            "time": ["2025-01-02T00:00:00+00:00", "2025-01-02T01:00:00+00:00"],
            "city": new_city,
            "temperature": [10.5, 11.0],
        }
    )
    csv_path = tmp_path / "test_data.csv"
    csv_data.to_csv(csv_path, index=False, sep=delimiter, encoding=encoding)

    new_data_count = len(csv_data)
    original_count = len(delta_table.to_pandas())

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "import",
            table_name,
            "--file",
            str(csv_path),
            "--mode",
            insert_mode,
            "--format",
            "csv",
            "--delimiter",
            delimiter,
            "--encoding",
            encoding,
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert (
        f"Successfully imported {new_data_count} rows into table '{table_name}' in '{insert_mode}' mode"
        in output
    )

    updated_table = deltalake.DeltaTable(delta_table.table_uri)
    updated_count = len(updated_table.to_pandas())
    assert updated_count == original_count + new_data_count

    df = updated_table.to_pandas()
    assert new_city in df["city"].unique()


def test_tables_import_csv_overwrite(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    table_name = sample_config["tables"][0]["name"]
    insert_mode = "overwrite"

    new_city = "Lyon"
    csv_data = pd.DataFrame(
        {
            "time": pd.to_datetime(
                ["2025-01-02T00:00:00+00:00", "2025-01-02T01:00:00+00:00"]
            ),
            "city": new_city,
            "temperature": [10.5, 11.0],
        }
    )
    csv_path = tmp_path / "test_data.csv"
    csv_data.to_csv(csv_path, index=False)
    csv_data_count = len(csv_data)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "import",
            table_name,
            "--file",
            str(csv_path),
            "--mode",
            insert_mode,
            "--format",
            "csv",
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert (
        f"Successfully imported {csv_data_count} rows into table '{table_name}' in '{insert_mode}' mode"
        in output
    )

    updated_table = deltalake.DeltaTable(delta_table.table_uri)
    updated_count = len(updated_table.to_pandas())
    assert updated_count == csv_data_count

    df = updated_table.to_pandas()
    df["time"] = pd.to_datetime(df["time"], utc=True)
    assert (df == csv_data).all().all()


def test_tables_import_csv_missing_file(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    csv_path = "nonexistent.csv"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "import",
            sample_config["tables"][0]["name"],
            "--file",
            csv_path,
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert f"No such file or directory: '{csv_path}'" in output


def test_tables_import_csv_invalid_table(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    invalid_table = sample_config["tables"][-1]

    csv_data = pd.DataFrame(
        {"time": ["2025-01-02T00:00:00+00:00"], "city": ["Lyon"], "temperature": [10.5]}
    )
    csv_path = tmp_path / "test_data.csv"
    csv_data.to_csv(csv_path, index=False)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "import",
            invalid_table["name"],
            "--file",
            str(csv_path),
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert f"Invalid table: {invalid_table['uri']}" in output


def test_tables_import_csv_schema_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    csv_data = pd.DataFrame(
        {"wrong_column": ["value1", "value2"], "another_wrong": ["value3", "value4"]}
    )
    csv_path = tmp_path / "test_data.csv"
    csv_data.to_csv(csv_path, index=False)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "laketower",
            "--config",
            str(sample_config_path),
            "tables",
            "import",
            sample_config["tables"][0]["name"],
            "--file",
            str(csv_path),
        ],
    )

    cli.cli()

    captured = capsys.readouterr()
    output = captured.out
    assert "Invariant violations" in output
