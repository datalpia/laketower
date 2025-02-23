import urllib.parse
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any
from unittest.mock import patch

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


@pytest.mark.parametrize(
    ("path", "args", "expected"),
    [
        (
            "/tables/table_name/view",
            {"sort_asc": "col1", "sort_desc": None},
            "/tables/table_name/view?sort_asc=col1",
        ),
        (
            "/tables/table_name/view?sort_asc=col1",
            {"sort_asc": None, "sort_desc": "col1"},
            "/tables/table_name/view?sort_desc=col1",
        ),
        (
            "/tables/table_name/view?limit=1&cols=col2&cols=col3",
            {"sort_asc": "col1", "sort_desc": None},
            "/tables/table_name/view?limit=1&cols=col2&cols=col3&sort_asc=col1",
        ),
    ],
)
def test_current_path_with_args(path: str, args: dict[str, str], expected: str) -> None:
    parsed = urllib.parse.urlparse(path)
    with patch("laketower.web.Request") as request_mock:
        request = request_mock.return_value
        request.query_params.multi_items.return_value = urllib.parse.parse_qsl(
            parsed.query
        )
        request.url.path = parsed.path
        assert web.current_path_with_args(request, args) == expected


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


def test_table_view(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]
    default_limit = 10

    response = client.get(f"/tables/{table['name']}/view")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert all(field.name in html for field in delta_table.schema().fields)
    assert all(
        f"/tables/{table['name']}/view?sort_asc={field.name}" in html
        for field in delta_table.schema().fields
    )

    df = delta_table.to_pandas()[0:default_limit]
    assert all(str(row[col]) in html for _, row in df.iterrows() for col in row.index)


def test_tables_view_limit(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]
    selected_limit = 1

    response = client.get(f"/tables/{table['name']}/view?limit={selected_limit}")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert all(field.name in html for field in delta_table.schema().fields)

    df = delta_table.to_pandas()[0:selected_limit]
    assert all(str(row[col]) in html for _, row in df.iterrows() for col in row.index)


def test_tables_view_cols(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]
    num_fields = len(delta_table.schema().fields)
    selected_columns = [
        delta_table.schema().fields[i].name for i in range(num_fields - 1)
    ]
    filtered_columns = [delta_table.schema().fields[num_fields - 1].name]
    qs = "&".join(f"cols={col}" for col in selected_columns)

    response = client.get(f"/tables/{table['name']}/view?{qs}")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert all(col in html for col in selected_columns)
    assert not all(col in html for col in filtered_columns)

    df = delta_table.to_pandas()
    assert all(str(row) in html for row in df[selected_columns])
    assert not all(str(row) in html for row in df[filtered_columns])


def test_table_view_sort_asc(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]
    default_limit = 10
    sort_column = "temperature"

    response = client.get(f"/tables/{table['name']}/view?sort_asc={sort_column}")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert all(field.name in html for field in delta_table.schema().fields)
    assert f"/tables/{table['name']}/view?sort_desc={sort_column}" in html
    assert all(
        f"/tables/{table['name']}/view?sort_asc={field.name}" in html
        for field in delta_table.schema().fields
        if field.name != sort_column
    )

    df = delta_table.to_pandas().sort_values(by=sort_column, ascending=True)[
        :default_limit
    ]
    assert all(str(row[col]) in html for _, row in df.iterrows() for col in row.index)


def test_table_view_sort_desc(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]
    default_limit = 10
    sort_column = "temperature"

    response = client.get(f"/tables/{table['name']}/view?sort_desc={sort_column}")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert all(field.name in html for field in delta_table.schema().fields)
    assert all(
        f"/tables/{table['name']}/view?sort_asc={field.name}" in html
        for field in delta_table.schema().fields
    )

    df = delta_table.to_pandas().sort_values(by=sort_column, ascending=False)[
        :default_limit
    ]
    assert all(str(row[col]) in html for _, row in df.iterrows() for col in row.index)
