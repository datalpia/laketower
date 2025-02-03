import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import deltalake
import pytest
import yaml

from laketower import cli


@pytest.fixture()
def delta_table(tmp_path: Path) -> deltalake.DeltaTable:
    table_path = tmp_path / "delta_table"
    schema = deltalake.Schema(
        [
            deltalake.Field("time", "timestamp", nullable=False),  # type: ignore[arg-type]
            deltalake.Field("city", "string", nullable=False),  # type: ignore[arg-type]
            deltalake.Field("temperature", "float", nullable=False),  # type: ignore[arg-type]
        ]
    )
    return deltalake.DeltaTable.create(
        table_path, schema, name="delta_table", description="Sample Delta Table"
    )


@pytest.fixture()
def sample_config(delta_table: deltalake.DeltaTable) -> dict[str, Any]:
    return {
        "tables": [
            {"name": "delta_table", "uri": delta_table.table_uri, "format": "delta"}
        ]
    }


@pytest.fixture()
def sample_config_path(tmp_path: Path, sample_config: dict[str, Any]) -> Path:
    config_path = tmp_path / "laketower.yml"
    config_path.write_text(yaml.dump(sample_config))
    return config_path


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


def test_tables_inspect(
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
            "inspect",
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
