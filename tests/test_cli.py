import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import deltalake
import pyarrow as pa
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


def test_config_validate_invalid_delta_table(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    sample_config: dict[str, Any],
) -> None:
    sample_config["tables"][0]["uri"] = "unknown_table"
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
    assert selected_column in output
    assert not all(col in output for col in filtered_columns)

    df = delta_table.to_pandas()
    assert all(str(row) in output for row in df[selected_column][0:selected_limit])
    assert not all(str(row) in output for row in df[selected_column][selected_limit:])


def test_tables_query_invalid(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
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
            "query",
            "select * from unknown_table",
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
    assert all(col in output for col in {"day", "temperature"})


def test_queries_view_invalid(
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
    assert not all(col in output for col in {"day", "temperature"})
