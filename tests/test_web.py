from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any

import deltalake
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from laketower import web


@pytest.fixture()
def app(monkeypatch: pytest.MonkeyPatch, sample_config_path: Path) -> FastAPI:
    monkeypatch.setenv("LAKETOWER_CONFIG_PATH", str(sample_config_path.absolute()))
    return web.create_app()


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def test_index(client: TestClient, sample_config: dict[str, Any]) -> None:
    response = client.get("/")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    for table in sample_config["tables"]:
        assert table["name"] in html
        assert f"/tables/{table['name']}" in html


def test_table_index(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]

    response = client.get(f"/tables/{table['name']}")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    for table in sample_config["tables"]:
        assert table["name"] in html

    assert "Schema" in html
    assert "Column" in html
    assert "Type" in html
    assert "Nullable" in html
    for field in delta_table.schema().to_pyarrow():
        assert field.name in html
        assert str(field.type) in html
        assert str(field.nullable) in html

    assert "Metadata" in html
    assert "Format" in html
    assert table["format"] in html
    assert "Name" in html
    assert delta_table.metadata().name in html
    assert "Description" in html
    assert delta_table.metadata().description in html
    assert "URI" in html
    assert table["uri"]
    assert "ID" in html
    assert str(delta_table.metadata().id) in html
    assert "Version" in html
    assert str(delta_table.version()) in html
    assert "Created at" in html
    assert (
        str(
            datetime.fromtimestamp(
                delta_table.metadata().created_time / 1000, tz=timezone.utc
            )
        )
        in html
    )
    assert "Partitions" in html
    assert ", ".join(delta_table.metadata().partition_columns) in html
    assert "Configuration" in html
    assert str(delta_table.metadata().configuration) in html


def test_table_history(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]

    response = client.get(f"/tables/{table['name']}/history")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    for rev in delta_table.history():
        assert f"version: {rev['version']}" in html
        assert (
            f"timestamp: {datetime.fromtimestamp(rev['timestamp'] / 1000, tz=timezone.utc)}"
            in html
        )
        assert f"client version: {rev['clientVersion']}" in html
        assert f"operation: {rev['operation']}" in html
        assert "operation parameters" in html
        operation_parameters = rev["operationParameters"]
        for param_key in operation_parameters.keys():
            assert f"{param_key}: " in html
        assert "operation metrics" in html
        operation_metrics = rev.get("operationMetrics")
        if operation_metrics:
            for metric_key, metric_val in operation_metrics.items():
                assert f"{metric_key}: {metric_val}" in html
