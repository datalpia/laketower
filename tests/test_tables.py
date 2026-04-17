import io
from pathlib import Path
from typing import Any
from unittest import mock

import pyarrow as pa
import pytest

from laketower import config, tables


def test_resolve_table_delta(sample_config_table_delta_s3: dict[str, Any]) -> None:
    table_config = config.ConfigTable.model_validate(sample_config_table_delta_s3)

    handler_class = tables.resolve_table(table_config)

    assert handler_class is tables.DeltaTable


@mock.patch("laketower.tables.deltalake.write_deltalake")
def test_deltatable_import_data_local(
    mock_write_deltalake: mock.MagicMock,
    tmp_path: Path,
) -> None:
    table_config = config.ConfigTable.model_validate(
        {"name": "local_table", "uri": str(tmp_path / "table"), "format": "delta"}
    )
    data = pa.table({"col1": [1, 2], "col2": ["a", "b"]})

    tables.DeltaTable.import_data(table_config, data, tables.ImportModeEnum.overwrite)

    assert mock_write_deltalake.call_count == 1
    assert mock_write_deltalake.call_args.kwargs["storage_options"] is None
    assert mock_write_deltalake.call_args.kwargs["mode"] == "overwrite"


@mock.patch("laketower.tables.deltalake.write_deltalake")
def test_deltatable_import_data_s3(
    mock_write_deltalake: mock.MagicMock,
    sample_config_table_delta_s3: dict[str, Any],
) -> None:
    table_config = config.ConfigTable.model_validate(sample_config_table_delta_s3)
    data = pa.table({"col1": [1, 2], "col2": ["a", "b"]})

    tables.DeltaTable.import_data(table_config, data, tables.ImportModeEnum.overwrite)

    expected_s3_conn = sample_config_table_delta_s3["storage_credential"]["s3"]
    assert mock_write_deltalake.call_count == 1
    assert mock_write_deltalake.call_args.kwargs["storage_options"] == {
        "aws_access_key_id": expected_s3_conn["access_key_id"],
        "aws_secret_access_key": expected_s3_conn["secret_access_key"],
        "aws_region": expected_s3_conn["region"],
        "aws_endpoint_url": str(expected_s3_conn["endpoint_url"]).rstrip("/"),
        "aws_allow_http": str(expected_s3_conn["allow_http"]).lower(),
    }
    assert mock_write_deltalake.call_args.kwargs["mode"] == "overwrite"


@mock.patch("laketower.tables.deltalake.write_deltalake")
def test_deltatable_import_data_adls(
    mock_write_deltalake: mock.MagicMock,
    sample_config_table_delta_adls: dict[str, Any],
) -> None:
    table_config = config.ConfigTable.model_validate(sample_config_table_delta_adls)
    data = pa.table({"col1": [1, 2], "col2": ["a", "b"]})

    tables.DeltaTable.import_data(table_config, data, tables.ImportModeEnum.append)

    expected_adls_conn = sample_config_table_delta_adls["storage_credential"]["adls"]
    assert mock_write_deltalake.call_count == 1
    assert mock_write_deltalake.call_args.kwargs["storage_options"] == {
        "azure_storage_account_name": expected_adls_conn["account_name"],
        "azure_storage_access_key": expected_adls_conn["access_key"],
        "azure_storage_sas_key": expected_adls_conn["sas_key"],
        "azure_storage_tenant_id": expected_adls_conn["tenant_id"],
        "azure_storage_client_id": expected_adls_conn["client_id"],
        "azure_storage_client_secret": expected_adls_conn["client_secret"],
        "azure_msi_endpoint": str(expected_adls_conn["msi_endpoint"]).rstrip("/"),
        "azure_use_azure_cli": str(expected_adls_conn["use_azure_cli"]).lower(),
    }
    assert mock_write_deltalake.call_args.kwargs["mode"] == "append"


@mock.patch("laketower.tables.deltalake.write_deltalake")
def test_import_file_to_table_nonexistent_table(
    mock_write_deltalake: mock.MagicMock,
    tmp_path: Path,
) -> None:
    table_config = config.ConfigTable.model_validate(
        {"name": "new_table", "uri": str(tmp_path / "new_table"), "format": "delta"}
    )
    csv_content = b"col1,col2\n1,a\n2,b\n"

    rows = tables.import_file_to_table(table_config, io.BytesIO(csv_content))

    assert rows == 2
    assert mock_write_deltalake.call_count == 1
    assert mock_write_deltalake.call_args.args[0] == str(tmp_path / "new_table")


@mock.patch(
    "laketower.tables.deltalake.DeltaTable.is_deltatable",
    side_effect=OSError("Generic S3 error"),
)
def test_is_valid_deltatable_os_error(
    mock_is_deltatable: mock.MagicMock, sample_config_table_delta_s3: dict[str, Any]
) -> None:
    table_config = config.ConfigTable.model_validate(sample_config_table_delta_s3)

    result = tables.DeltaTable.is_valid(table_config)

    assert result is False


@mock.patch("laketower.tables.deltalake.DeltaTable")
def test_load_table_deltatable_s3(
    mock_deltatable: mock.MagicMock, sample_config_table_delta_s3: dict[str, Any]
) -> None:
    table_config = config.ConfigTable.model_validate(sample_config_table_delta_s3)

    _ = tables.load_table(table_config)

    expected_s3_conn = sample_config_table_delta_s3["storage_credential"]["s3"]
    assert mock_deltatable.call_count == 1
    assert mock_deltatable.call_args.kwargs["storage_options"] == {
        "aws_access_key_id": expected_s3_conn["access_key_id"],
        "aws_secret_access_key": expected_s3_conn["secret_access_key"],
        "aws_region": expected_s3_conn["region"],
        "aws_endpoint_url": str(expected_s3_conn["endpoint_url"]).rstrip("/"),
        "aws_allow_http": str(expected_s3_conn["allow_http"]).lower(),
    }


@mock.patch("laketower.tables.deltalake.DeltaTable")
def test_load_table_deltatable_adls(
    mock_deltatable: mock.MagicMock, sample_config_table_delta_adls: dict[str, Any]
) -> None:
    table_config = config.ConfigTable.model_validate(sample_config_table_delta_adls)

    _ = tables.load_table(table_config)

    expected_adls_conn = sample_config_table_delta_adls["storage_credential"]["adls"]
    assert mock_deltatable.call_count == 1
    assert mock_deltatable.call_args.kwargs["storage_options"] == {
        "azure_storage_account_name": expected_adls_conn["account_name"],
        "azure_storage_access_key": expected_adls_conn["access_key"],
        "azure_storage_sas_key": expected_adls_conn["sas_key"],
        "azure_storage_tenant_id": expected_adls_conn["tenant_id"],
        "azure_storage_client_id": expected_adls_conn["client_id"],
        "azure_storage_client_secret": expected_adls_conn["client_secret"],
        "azure_msi_endpoint": str(expected_adls_conn["msi_endpoint"]).rstrip("/"),
        "azure_use_azure_cli": str(expected_adls_conn["use_azure_cli"]).lower(),
    }


@pytest.mark.parametrize(
    ("sql", "names"),
    [
        ("select * from table", set()),
        ("select * from table where col = $val", {"val"}),
        ("select * from table where col1 = $val1 and col2 = $val2", {"val1", "val2"}),
    ],
)
def test_extract_query_parameter_names(sql: str, names: set[str]) -> None:
    param_names = tables.extract_query_parameter_names(sql)
    assert param_names == names


@pytest.mark.parametrize("sql", ["select * from", 'select * from "t'])
def test_extract_query_parameter_names_invalid_sql(sql: str) -> None:
    with pytest.raises(ValueError):
        tables.extract_query_parameter_names(sql)


@pytest.mark.parametrize(
    ["table_name", "limit", "cols", "sort_asc", "sort_desc", "expected_query"],
    [
        ("test_table", None, None, None, None, 'SELECT * FROM "test_table" LIMIT 10'),
        ("test_table", 5, None, None, None, 'SELECT * FROM "test_table" LIMIT 5'),
        (
            "test_table",
            5,
            ["col1", "col2"],
            "col1",
            None,
            'SELECT "col1", "col2" FROM "test_table" ORDER BY "col1" ASC NULLS FIRST LIMIT 5',
        ),
        (
            "test_table",
            5,
            ["col1", "col2"],
            None,
            "col1",
            'SELECT "col1", "col2" FROM "test_table" ORDER BY "col1" DESC LIMIT 5',
        ),
        ("123_table", None, None, None, None, 'SELECT * FROM "123_table" LIMIT 10'),
        (
            "123_table",
            None,
            ["123_col"],
            None,
            None,
            'SELECT "123_col" FROM "123_table" LIMIT 10',
        ),
    ],
)
def test_generate_table_query_success(
    table_name: str,
    limit: int | None,
    cols: list[str] | None,
    sort_asc: str | None,
    sort_desc: str | None,
    expected_query: str,
) -> None:
    query = tables.generate_table_query(table_name, limit, cols, sort_asc, sort_desc)
    assert query == expected_query


@pytest.mark.parametrize("table_name", ["test_table", "123_table"])
def test_generate_table_statistics_query_success(table_name: str) -> None:
    expected_query = f'SELECT "column_name", "count", "avg", "std", "min", "max" FROM (SUMMARIZE "{table_name}")'
    query = tables.generate_table_statistics_query(table_name)
    assert query == expected_query


@pytest.mark.parametrize(
    ("sql_query", "max_limit", "expected"),
    [
        (
            'SELECT * FROM "test_table"',
            1_000,
            'SELECT * FROM (SELECT * FROM "test_table") LIMIT 1000',
        ),
        (
            'SELECT * FROM "test_table" LIMIT 100',
            1_000,
            'SELECT * FROM (SELECT * FROM "test_table" LIMIT 100) LIMIT 1000',
        ),
        (
            'SELECT * FROM "test_table" LIMIT 2000',
            1_000,
            'SELECT * FROM (SELECT * FROM "test_table" LIMIT 2000) LIMIT 1000',
        ),
        (
            'SELECT * FROM "test_table" LIMIT (10 + 5)',
            1_000,
            'SELECT * FROM (SELECT * FROM "test_table" LIMIT (10 + 5)) LIMIT 1000',
        ),
        (
            'SELECT * FROM "test_table" LIMIT ALL',
            1_000,
            'SELECT * FROM (SELECT * FROM "test_table" LIMIT "ALL") LIMIT 1000',
        ),
        (
            'SELECT * FROM "test_table" LIMIT $param',
            1_000,
            'SELECT * FROM (SELECT * FROM "test_table" LIMIT $param) LIMIT 1000',
        ),
        (
            'SELECT * FROM "test_table" LIMIT some_var',
            1_000,
            'SELECT * FROM (SELECT * FROM "test_table" LIMIT "some_var") LIMIT 1000',
        ),
        (
            'SELECT * FROM "test_table" LIMIT (SELECT max("id") FROM "other_table")',
            1_000,
            'SELECT * FROM (SELECT * FROM "test_table" LIMIT (SELECT MAX("id") FROM "other_table")) LIMIT 1000',
        ),
        (
            'WITH "some_cte" AS (SELECT * FROM "other_table") SELECT * FROM "test_table"',
            1_000,
            'SELECT * FROM (WITH "some_cte" AS (SELECT * FROM "other_table") SELECT * FROM "test_table") LIMIT 1000',
        ),
        (
            'CREATE MACRO preprocessing(s) AS trim(s); SELECT * FROM "test_table"',
            1_000,
            'CREATE MACRO "preprocessing"(s) AS TRIM("s"); SELECT * FROM (SELECT * FROM "test_table") LIMIT 1000',
        ),
        (
            "CREATE MACRO preprocessing(s) AS trim(s)",
            1_000,
            'CREATE MACRO "preprocessing"(s) AS TRIM("s")',
        ),
        (
            "",
            1_000,
            "",
        ),
    ],
)
def test_limit_query(sql_query: str, max_limit: int, expected: str) -> None:
    result = tables.limit_query(sql_query, max_limit)
    assert result == expected


@pytest.mark.parametrize("sql", ["select * from", 'select * from "t'])
def test_limit_query_invalid_sql(sql: str) -> None:
    with pytest.raises(ValueError):
        tables.limit_query(sql, 1_000)


def test_compute_totals() -> None:
    data = pa.table({"col1": ["cat1", "cat2", "cat3"], "col2": [1, 2, 3]})

    totals = tables.compute_totals(data)

    assert totals.num_rows == 1
    assert totals.schema == data.schema
    assert totals["col1"][0].as_py() is None
    assert totals["col2"][0].as_py() == 6
