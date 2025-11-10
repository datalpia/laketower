import urllib.parse
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any
from unittest.mock import patch

import deltalake
import pandas as pd
import pyarrow as pa
import pytest
import yaml
from bs4 import BeautifulSoup
from fastapi import FastAPI
from fastapi.testclient import TestClient

from laketower import __about__, web


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
            [("sort_asc", "col1"), ("sort_desc", None)],
            "/tables/table_name/view?sort_asc=col1",
        ),
        (
            "/tables/table_name/view?sort_asc=col1",
            [("sort_asc", None), ("sort_desc", "col1")],
            "/tables/table_name/view?sort_desc=col1",
        ),
        (
            "/tables/table_name/view?limit=1&cols=col2&cols=col3",
            [("sort_asc", "col1"), ("sort_desc", None)],
            "/tables/table_name/view?limit=1&cols=col2&cols=col3&sort_asc=col1",
        ),
    ],
)
def test_current_path_with_args(
    path: str, args: list[tuple[str, str]], expected: str
) -> None:
    parsed = urllib.parse.urlparse(path)
    with patch("laketower.web.Request") as request_mock:
        request = request_mock.return_value
        request.query_params.multi_items.return_value = urllib.parse.parse_qsl(
            parsed.query
        )
        request.url.path = parsed.path
        assert web.current_path_with_args(request, args) == expected


@pytest.mark.parametrize(
    ("md_text", "expected_html"),
    [
        ("Some plain text", "<p>Some plain text</p>"),
        (
            "Some `Markdown` text with **strong** and _emphasized_ words",
            "<p>Some <code>Markdown</code> text with <strong>strong</strong> and <em>emphasized</em> words</p>",
        ),
        (
            "Some plain text with xss<script>alert('hello')</script>",
            "<p>Some plain text with xss&lt;script&gt;alert('hello')&lt;/script&gt;</p>",
        ),
    ],
)
def test_render_markdown(md_text: str, expected_html: str) -> None:
    assert web.render_markdown(md_text) == expected_html


def test_index(client: TestClient, sample_config: dict[str, Any]) -> None:
    response = client.get("/")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert "🗼 Laketower" in html
    assert f"{__about__.__version__}" in html
    assert (
        f"https://github.com/datalpia/laketower/releases/tag/{__about__.__version__}"
        in html
    )

    for table in sample_config["tables"]:
        assert table["name"] in html
        assert f"/tables/{table['name']}" in html
    for query in sample_config["queries"]:
        assert query["title"] in html
        assert f"/queries/{query['name']}/view" in html
        assert query["sql"] not in html


def test_index_hide_tables(
    monkeypatch: pytest.MonkeyPatch,
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    sample_config["settings"]["web"]["hide_tables"] = True
    sample_config_path.write_text(yaml.dump(sample_config))
    monkeypatch.setenv("LAKETOWER_CONFIG_PATH", str(sample_config_path.absolute()))
    client = TestClient(web.create_app())

    response = client.get("/")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")

    assert not soup.find("h6", string="Tables")  # type: ignore[call-overload]
    assert soup.find("h6", string="Queries")  # type: ignore[call-overload]

    for table in sample_config["tables"]:
        assert not next(
            filter(
                lambda a: a.get("href") == f"/tables/{table['name']}",
                soup.find_all("a"),
            ),
            None,
        )

    for query in sample_config["queries"]:
        assert next(
            filter(
                lambda a: a.get("href") == f"/queries/{query['name']}/view",
                soup.find_all("a"),
            ),
            None,
        )


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

    table_schema = pa.schema(delta_table.schema().to_arrow())  # type: ignore[arg-type]
    for field in table_schema:
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


def test_table_index_invalid_table_uri(
    client: TestClient, sample_config: dict[str, Any]
) -> None:
    table = sample_config["tables"][-1]

    response = client.get(f"/tables/{table['name']}")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    for table in sample_config["tables"]:
        assert table["name"] in html

    assert f"Invalid table: {table['uri']}" in html


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


def test_table_history_invalid_table_uri(
    client: TestClient, sample_config: dict[str, Any]
) -> None:
    table = sample_config["tables"][-1]

    response = client.get(f"/tables/{table['name']}/history")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert f"Invalid table: {table['uri']}" in html


def test_tables_statistics(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]

    response = client.get(f"/tables/{table['name']}/statistics")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert all(field.name in html for field in delta_table.schema().fields)
    assert "column_name" in html
    assert "count" in html
    assert "avg" in html
    assert "std" in html
    assert "min" in html
    assert "max" in html


def test_tables_statistics_version(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]

    response = client.get(f"/tables/{table['name']}/statistics", params={"version": 0})
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert all(field.name in html for field in delta_table.schema().fields)
    assert "column_name" in html
    assert "count" in html
    assert "avg" in html
    assert "std" in html
    assert "min" in html
    assert "max" in html


def test_table_statistics_invalid_table_uri(
    client: TestClient, sample_config: dict[str, Any]
) -> None:
    table = sample_config["tables"][-1]

    response = client.get(f"/tables/{table['name']}/statistics")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert f"Invalid table: {table['uri']}" in html


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

    response = client.get(
        f"/tables/{table['name']}/view", params={"limit": selected_limit}
    )
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert all(field.name in html for field in delta_table.schema().fields)

    df = delta_table.to_pandas()[0:selected_limit]
    assert all(str(row[col]) in html for _, row in df.iterrows() for col in row.index)


def test_tables_view_cols(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]
    default_limit = 10
    num_fields = len(delta_table.schema().fields)
    selected_columns = [
        delta_table.schema().fields[i].name for i in range(num_fields - 1)
    ]
    filtered_columns = [delta_table.schema().fields[num_fields - 1].name]

    response = client.get(
        f"/tables/{table['name']}/view",
        params=[("cols", col) for col in selected_columns],
    )
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")

    all_th = [th.get_text().strip() for th in soup.find_all("th")]
    assert all(col in all_th for col in selected_columns)
    assert not all(col in all_th for col in filtered_columns)

    all_td = [td.get_text().strip() for td in soup.find_all("td")]
    df = delta_table.to_pandas()[0:default_limit]
    assert all(
        str(row[col]) in all_td
        for _, row in df[selected_columns].iterrows()
        for col in row.index
    )
    assert not all(
        str(row[col]) in all_td
        for _, row in df[filtered_columns].iterrows()
        for col in row.index
    )


def test_table_view_sort_asc(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]
    default_limit = 10
    sort_column = "temperature"

    response = client.get(
        f"/tables/{table['name']}/view", params={"sort_asc": sort_column}
    )
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

    response = client.get(
        f"/tables/{table['name']}/view", params={"sort_desc": sort_column}
    )
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


def test_tables_view_version(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    table = sample_config["tables"][0]
    default_limit = 10

    response = client.get(f"/tables/{table['name']}/view", params={"version": 0})
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert all(field.name in html for field in delta_table.schema().fields)

    df = delta_table.to_pandas()[0:default_limit]
    assert not all(
        str(row[col]) in html for _, row in df.iterrows() for col in row.index
    )


def test_table_view_invalid_table_uri(
    client: TestClient, sample_config: dict[str, Any]
) -> None:
    table = sample_config["tables"][-1]

    response = client.get(f"/tables/{table['name']}/view")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert f"Invalid table: {table['uri']}" in html


def test_tables_query(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    selected_column = delta_table.schema().fields[0].name
    filtered_columns = [field.name for field in delta_table.schema().fields[1:]]
    selected_limit = 1
    sql_query = f"select {selected_column} from {sample_config['tables'][0]['name']} limit {selected_limit}"

    response = client.get("/tables/query", params={"sql": sql_query})
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")

    assert soup.find("h2", string="SQL Query")  # type: ignore[call-overload]
    assert (
        textarea := soup.find("textarea")
    ) and textarea.get_text().strip() == sql_query
    assert next(
        filter(lambda a: a.get_text().strip() == "Execute", soup.find_all("button"))
    )

    assert next(
        filter(
            lambda p: f"{selected_limit} rows returned" in p.get_text().strip(),
            soup.find_all("p"),
        )
    )
    assert next(
        filter(
            lambda p: "Query execution time: " in p.get_text().strip(),
            soup.find_all("p"),
        )
    )
    export_csv_a = next(
        filter(lambda a: a.get_text().strip() == "Export CSV", soup.find_all("a"))
    )
    assert (
        export_csv_a.get("href")
        == f"/tables/query/csv?sql={urllib.parse.quote(sql_query)}"
    )

    all_th = [th.get_text().strip() for th in soup.find_all("th")]
    assert selected_column in all_th
    assert not all(col in all_th for col in filtered_columns)

    df = delta_table.to_pandas()
    all_td = [td.get_text().strip() for td in soup.find_all("td")]
    assert all(str(row) in all_td for row in df[selected_column][0:selected_limit])
    assert not all(str(row) in all_td for row in df[selected_column][selected_limit:])


def test_tables_query_max_rows_limit(
    monkeypatch: pytest.MonkeyPatch,
    sample_config: dict[str, Any],
    sample_config_path: Path,
    delta_table: deltalake.DeltaTable,
) -> None:
    max_query_rows = 3
    sample_config["settings"]["max_query_rows"] = max_query_rows
    sample_config_path.write_text(yaml.dump(sample_config))
    monkeypatch.setenv("LAKETOWER_CONFIG_PATH", str(sample_config_path.absolute()))
    client = TestClient(web.create_app())

    selected_column = delta_table.schema().fields[0].name
    sql_query = f"select {selected_column} from {sample_config['tables'][0]['name']}"

    response = client.get("/tables/query", params={"sql": sql_query})
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")

    assert next(
        filter(
            lambda a: f"{max_query_rows} rows returned (truncated)"
            in a.get_text().strip(),
            soup.find_all("p"),
        )
    )
    assert next(
        filter(
            lambda p: "Query execution time: " in p.get_text().strip(),
            soup.find_all("p"),
        )
    )
    assert (table := soup.find("tbody"))
    assert len(table.find_all("tr", recursive=False)) == max_query_rows


@pytest.mark.parametrize(
    ("start_date", "end_date"),
    [("", ""), ("2025-01-01", ""), ("", "2025-01-31"), ("2025-01-01", "2025-01-31")],
)
def test_tables_query_parameters(
    client: TestClient,
    sample_config: dict[str, Any],
    start_date: str,
    end_date: str,
) -> None:
    sql_query = f"select * from {sample_config['tables'][0]['name']} where time between $start_date and $end_date"

    response = client.get(
        "/tables/query",
        params={"sql": sql_query, "start_date": start_date, "end_date": end_date},
    )
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")

    assert soup.find("h3", string="Parameters")  # type: ignore[call-overload]
    assert soup.find("label", string="start_date")  # type: ignore[call-overload]
    assert soup.find("input", attrs={"name": "start_date", "value": start_date})
    assert soup.find("label", string="end_date")  # type: ignore[call-overload]
    assert soup.find("input", attrs={"name": "end_date", "value": end_date})


@pytest.mark.parametrize(
    "sql_query",
    ["select * from unknown_table", "select * from", "select *", "select", ""],
)
def test_tables_query_invalid(
    client: TestClient, delta_table: deltalake.DeltaTable, sql_query: str
) -> None:
    response = client.get("/tables/query", params={"sql": sql_query})
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")

    assert soup.find("h2", string="SQL Query")  # type: ignore[call-overload]
    assert (
        textarea := soup.find("textarea")
    ) and textarea.get_text().strip() == sql_query
    assert next(
        filter(lambda a: a.get_text().strip() == "Execute", soup.find_all("button"))
    )
    assert "Error" in html

    assert (
        next(filter(lambda a: "Export CSV" in a.text, soup.find_all("a")), None) is None
    )

    all_th = [th.get_text().strip() for th in soup.find_all("th")]
    assert not all(field.name in all_th for field in delta_table.schema().fields)

    df = delta_table.to_pandas()
    all_td = [td.get_text().strip() for td in soup.find_all("td")]
    assert not all(
        str(row[col]) in all_td for _, row in df.iterrows() for col in row.index
    )


def test_tables_import(client: TestClient, sample_config: dict[str, Any]) -> None:
    table = sample_config["tables"][0]
    url = f"/tables/{table['name']}/import"

    response = client.get(url)
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")
    import_tab = next(
        filter(lambda a: a.get_text().strip() == "Import", soup.find_all("a")), None
    )
    assert import_tab is not None
    assert import_tab.get("href") == url
    assert (classes := import_tab.get("class")) and "active" in classes

    form = soup.find("form")
    assert form is not None
    assert form.get("action") == url
    assert form.get("method") == "post"
    assert form.get("enctype") == "multipart/form-data"

    file_input = soup.find("input", {"type": "file", "name": "input_file"})
    assert file_input is not None
    assert file_input.get("accept") == ".csv"
    assert file_input.has_attr("required")

    expected_mode_inputs = [("append", True), ("overwrite", False)]
    mode_inputs = soup.find_all("input", {"name": "mode"})
    for mode_input, expected_mode_input in zip(
        mode_inputs, expected_mode_inputs, strict=True
    ):
        assert mode_input.get("value") == expected_mode_input[0]
        assert mode_input.has_attr("checked") == expected_mode_input[1]

    expected_file_formats_options = [("csv", True)]
    file_format_select = soup.find("select", {"name": "file_format"})
    assert file_format_select is not None
    file_format_options = file_format_select.find_all("option", recursive=False)
    for file_format_option, expected_file_format_option in zip(
        file_format_options, expected_file_formats_options, strict=True
    ):
        assert file_format_option.get("value") == expected_file_format_option[0]
        assert file_format_option.has_attr("selected") == expected_file_format_option[1]

    delimiter_input = soup.find("input", {"name": "delimiter"})
    assert delimiter_input is not None
    assert delimiter_input.get("value") == ","
    assert delimiter_input.has_attr("required")

    expected_encoding_options = [
        ("utf-8", True),
        ("utf-16", False),
        ("utf-32", False),
        ("latin-1", False),
    ]
    encoding_select = soup.find("select", {"name": "encoding"})
    assert encoding_select is not None
    encoding_options = encoding_select.find_all("option", recursive=False)
    for encoding_option, expected_encoding_option in zip(
        encoding_options, expected_encoding_options, strict=True
    ):
        assert encoding_option.get("value") == expected_encoding_option[0]
        assert encoding_option.has_attr("selected") == expected_encoding_option[1]

    submit_button = soup.find("button", {"type": "submit"})
    assert submit_button is not None


def test_tables_import_invalid_table_uri(
    client: TestClient, sample_config: dict[str, Any]
) -> None:
    table = sample_config["tables"][-1]

    response = client.get(f"/tables/{table['name']}/import")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert f"Invalid table: {table['uri']}" in html


@pytest.mark.parametrize("delimiter", [",", ";"])
@pytest.mark.parametrize("encoding", ["utf-8", "latin-1"])
def test_tables_import_post_csv_append(
    client: TestClient,
    sample_config: dict[str, Any],
    delta_table: deltalake.DeltaTable,
    tmp_path: Path,
    delimiter: str,
    encoding: str,
) -> None:
    table = sample_config["tables"][0]

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

    response = client.post(
        f"/tables/{table['name']}/import",
        files={"input_file": open(csv_path, "rb")},
        data={
            "mode": "append",
            "file_format": "csv",
            "delimiter": delimiter,
            "encoding": encoding,
        },
    )
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert f"Successfully imported {new_data_count} rows" in html

    updated_table = deltalake.DeltaTable(table["uri"])
    new_count = len(updated_table.to_pandas())
    assert new_count == original_count + new_data_count


@pytest.mark.parametrize("delimiter", [",", ";"])
@pytest.mark.parametrize("encoding", ["utf-8", "latin-1"])
def test_tables_import_post_csv_overwrite(
    client: TestClient,
    sample_config: dict[str, Any],
    tmp_path: Path,
    delimiter: str,
    encoding: str,
) -> None:
    table = sample_config["tables"][0]

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

    response = client.post(
        f"/tables/{table['name']}/import",
        files={"input_file": open(csv_path, "rb")},
        data={
            "mode": "overwrite",
            "file_format": "csv",
            "delimiter": delimiter,
            "encoding": encoding,
        },
    )
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert f"Successfully imported {new_data_count} rows" in html

    updated_table = deltalake.DeltaTable(table["uri"])
    new_count = len(updated_table.to_pandas())
    assert new_count == new_data_count


def test_tables_import_post_csv_schema_mismatch(
    client: TestClient, sample_config: dict[str, Any], tmp_path: Path
) -> None:
    table = sample_config["tables"][0]

    csv_data = pd.DataFrame(
        {"wrong_column": ["value1", "value2"], "another_wrong": ["value3", "value4"]}
    )
    csv_path = tmp_path / "test_data.csv"
    csv_data.to_csv(csv_path, index=False)

    response = client.post(
        f"/tables/{table['name']}/import",
        files={"input_file": open(csv_path, "rb")},
        data={
            "file_format": "csv",
            "mode": "append",
            "delimiter": ",",
            "encoding": "utf-8",
        },
    )
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    assert "Invariant violations" in html


def test_queries_view(client: TestClient, sample_config: dict[str, Any]) -> None:
    query = sample_config["queries"][0]

    response = client.get(f"/queries/{query['name']}/view")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")

    assert soup.find("h2", string=query["title"])
    assert soup.find("textarea") is None
    assert next(
        filter(lambda a: a.get_text().strip() == "Edit SQL", soup.find_all("a"))
    )
    assert next(
        filter(lambda a: a.get_text().strip() == "Execute", soup.find_all("button"))
    )

    assert next(
        filter(
            lambda a: "rows returned" in a.get_text().strip(),
            soup.find_all("p"),
        )
    )
    assert next(
        filter(
            lambda p: "Query execution time: " in p.get_text().strip(),
            soup.find_all("p"),
        )
    )
    export_csv_a = next(
        filter(lambda a: a.get_text().strip() == "Export CSV", soup.find_all("a"))
    )
    assert (
        export_csv_a.get("href")
        == f"/tables/query/csv?sql={urllib.parse.quote(query['sql'])}"
    )

    all_th = [th.get_text().strip() for th in soup.find_all("th")]
    assert all(col in all_th for col in {"day", "avg_temperature"})


def test_queries_view_max_row_limit(
    monkeypatch: pytest.MonkeyPatch,
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    max_query_rows = 3
    sample_config["settings"]["max_query_rows"] = max_query_rows
    sample_config_path.write_text(yaml.dump(sample_config))
    monkeypatch.setenv("LAKETOWER_CONFIG_PATH", str(sample_config_path.absolute()))
    client = TestClient(web.create_app())

    query = sample_config["queries"][0]

    response = client.get(f"/queries/{query['name']}/view")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")

    assert next(
        filter(
            lambda a: f"{max_query_rows} rows returned (truncated)"
            in a.get_text().strip(),
            soup.find_all("p"),
        )
    )
    assert next(
        filter(
            lambda p: "Query execution time: " in p.get_text().strip(),
            soup.find_all("p"),
        )
    )
    assert (table := soup.find("tbody"))
    assert len(table.find_all("tr", recursive=False)) == max_query_rows


def test_queries_view_parameters_default(
    client: TestClient, sample_config: dict[str, Any]
) -> None:
    query = sample_config["queries"][1]

    response = client.get(f"/queries/{query['name']}/view")
    assert response.status_code == HTTPStatus.OK
    assert response.history[0].has_redirect_location

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")

    assert soup.find("h2", string=query["title"])
    assert soup.find("h3", string="Parameters")  # type: ignore[call-overload]

    for param_name, param_data in query["parameters"].items():
        assert soup.find("label", string=param_name)
        assert soup.find(
            "input", attrs={"name": param_name, "value": param_data["default"]}
        )


def test_queries_view_parameters(
    client: TestClient, sample_config: dict[str, Any]
) -> None:
    query = sample_config["queries"][1]

    response = client.get(
        f"/queries/{query['name']}/view",
        params={k: v["default"] for k, v in query["parameters"].items()},
    )
    assert response.status_code == HTTPStatus.OK
    assert len(response.history) == 0

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")

    assert soup.find("h2", string=query["title"])
    assert soup.find("h3", string="Parameters")  # type: ignore[call-overload]

    for param_name, param_data in query["parameters"].items():
        assert soup.find("label", string=param_name)
        assert soup.find(
            "input", attrs={"name": param_name, "value": param_data["default"]}
        )


def test_queries_view_invalid(
    monkeypatch: pytest.MonkeyPatch,
    sample_config: dict[str, Any],
    sample_config_path: Path,
) -> None:
    sample_config["queries"][0]["sql"] = "select * from unknown_table"
    query = sample_config["queries"][0]
    sample_config_path.write_text(yaml.dump(sample_config))
    monkeypatch.setenv("LAKETOWER_CONFIG_PATH", str(sample_config_path.absolute()))
    client = TestClient(web.create_app())

    response = client.get(f"/queries/{query['name']}/view")
    assert response.status_code == HTTPStatus.OK

    html = response.content.decode()
    soup = BeautifulSoup(html, "html.parser")

    assert soup.find("h2", string=query["title"])
    assert soup.find("textarea") is None
    assert next(
        filter(lambda a: a.get_text().strip() == "Edit SQL", soup.find_all("a"))
    )
    assert next(
        filter(lambda a: a.get_text().strip() == "Execute", soup.find_all("button"))
    )

    assert (
        next(filter(lambda a: "Export CSV" in a.text, soup.find_all("a")), None) is None
    )

    all_th = [th.get_text().strip() for th in soup.find_all("th")]
    assert not all(col in all_th for col in {"day", "avg_temperature"})


def test_tables_query_export_csv(
    client: TestClient, sample_config: dict[str, Any], delta_table: deltalake.DeltaTable
) -> None:
    selected_column = delta_table.schema().fields[0].name
    selected_limit = 2
    sql_query = f"select {selected_column} from {sample_config['tables'][0]['name']} limit {selected_limit}"

    response = client.get("/tables/query/csv", params={"sql": sql_query})
    assert response.status_code == HTTPStatus.OK
    assert response.headers["content-type"] == "text/csv; charset=utf-8"
    assert "attachment" in response.headers["content-disposition"]
    assert "query_results.csv" in response.headers["content-disposition"]

    csv_content = response.content.decode("utf-8")
    assert selected_column in csv_content
    lines = csv_content.strip().split("\n")
    assert len(lines) == selected_limit + 1


def test_queries_view_export_csv(
    client: TestClient, sample_config: dict[str, Any]
) -> None:
    query = sample_config["queries"][0]

    response = client.get("/tables/query/csv", params={"sql": query["sql"]})
    assert response.status_code == HTTPStatus.OK
    assert response.headers["content-type"] == "text/csv; charset=utf-8"
    assert "attachment" in response.headers["content-disposition"]
    assert "query_results.csv" in response.headers["content-disposition"]

    csv_content = response.content.decode("utf-8")
    assert "day" in csv_content
    assert "avg_temperature" in csv_content
    lines = csv_content.strip().split("\n")
    assert len(lines) > 1
