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
